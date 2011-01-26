/**
 * Copyright 2010 Twitter
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author Travis Crawford <travis@twitter.com>
 */

#include "source.h"
#include "scribe_server.h"

#ifdef HAVE_INOTIFY
#include <sys/inotify.h>
#endif

using boost::shared_ptr;
using boost::property_tree::ptree;
using namespace scribe::thrift;
using namespace std;

extern shared_ptr<scribeHandler> g_Handler;

void* sourceStarter(void *this_ptr) {
  Source *source_ptr = (Source*)this_ptr;
  source_ptr->run();
  return NULL;
}

bool Source::createSource(ptree& conf, shared_ptr<Source>& newSource) {
  string type = conf.get<string>("type", "");
  if (0 == type.compare("tail")) {
    newSource = shared_ptr<Source>(new TailSource(conf));
    return true;
  } else {
    LOG_OPER("Unable to create source for unknown type <%s>", type.c_str());
    return false;
  }
}

Source::Source(ptree& conf) {
  configuration = conf;
  validConfiguration = true;
}

Source::~Source() {}

void Source::configure() {
  categoryHandled = configuration.get<string>("category", "");
  if (categoryHandled == "") {
    LOG_OPER("Invalid Source configuration! No <category> specified.");
    validConfiguration = false;
  }
}

void Source::start() {}

void Source::stop() {}

void Source::run() {}


TailSource::TailSource(ptree& configuration) : Source(configuration) {
#ifdef HAVE_INOTIFY
    inotify_fd = inotify_init();
    if (inotify_fd < 0) {
        LOG_OPER("inotify_init failed: %s", strerror(errno));
    }
#endif
}

TailSource::~TailSource() {
#ifdef HAVE_INOTIFY
  if (inotify_fd >= 0) {
    close(inotify_fd);
  }
#endif
}

void TailSource::configure() {
  Source::configure();
  filename = configuration.get<string>("file", "");
  if (filename == "") {
    LOG_OPER("[%s] Invalid TailSource configuration! No <file> specified.",
      categoryHandled.c_str());
    validConfiguration = false;
  } else {
#ifdef HAVE_INOTIFY
    int rv = inotify_add_watch(inotify_fd, filename.c_str(), IN_MODIFY);
    if (rv < 0) {
      LOG_OPER("Failed to add inotify watch for file %s: %s",
        filename.c_str(), strerror(errno));
      validConfiguration = false;
    }
#endif
  }
}

void TailSource::start() {
  active = true;
  pthread_create(&sourceThread, NULL, sourceStarter, (void*) this);
}

void TailSource::stop() {
  active = false;
  pthread_join(sourceThread, NULL);
}

void TailSource::run() {

  configure();
  if (!validConfiguration) {
    return;
  }

  LOG_OPER("[%s] Starting tail source for file <%s>",
    categoryHandled.c_str(), filename.c_str());

  boost::iostreams::file_source logFile(filename.c_str());
  in.push(logFile);

  struct stat previousStat;
  stat(filename.c_str(), &previousStat);
  struct stat currentStat = previousStat;

  boost::iostreams::seek(in, 0, BOOST_IOS::end);
  string line;

  vector<LogEntry> messages;

  while (active) {
#ifdef HAVE_INOFITY
    struct inotify_event event;
    memset(&event, 0, sizeof(event));
    int rv = read(inotify_fd, &event, sizeof(event));
    LOG_DEBUG("Detected change in file %s", filename.c_str());
    if (rv < 0) {
        LOG_OPER("Failed to read inotify event for file %s: %s", filename.c_str(), strerror(errno));
        sleep(10);
        continue;
    }
#else
    // inotify is not available. Sleep for one second before checking the file.
    sleep(1);
#endif


    stat(filename.c_str(), &currentStat);

    // Files sometimes have their inode changed, such as during some types
    // of log rotation. Always follow the current file.
    if (currentStat.st_ino != previousStat.st_ino) {
      LOG_DEBUG("[%s] File <%s> inode changed! Continuing to follow the named file.",
        categoryHandled.c_str(), filename.c_str());
      in.reset();
      boost::iostreams::file_source logFile(filename.c_str());
      in.push(logFile);
    }

    // If the file is smaller its probably because of truncation; its common
    // for logs to be copied+truncated during rotation.
    else if (currentStat.st_size < previousStat.st_size) {
      LOG_DEBUG("[%s] File <%s> shrank! Assuming truncation and rewinding.",
        categoryHandled.c_str(), filename.c_str());
      boost::iostreams::seek(in, 0, BOOST_IOS::beg);
    }
    previousStat = currentStat;

    if (in.good()) {
      getline(in, line);
      if (!line.empty()) {
        LogEntry logEntry = LogEntry();
        logEntry.category = categoryHandled;
        logEntry.message = line + "\n";
        messages.push_back(logEntry);

        ResultCode rc = g_Handler->Log(messages);
        if (rc == scribe::thrift::OK) {
          //LOG_DEBUG("[%s] Successfully logged <%d> tailed messages from <%s>.",
          //    categoryHandled.c_str(), (int) messages.size(), filename.c_str());
          g_Handler->incCounter(categoryHandled, "tail good", messages.size());
        } else if (rc == scribe::thrift::TRY_LATER) {
          // TODO(travis): Add actual error handling.
          LOG_DEBUG("[%s] Failed logging <%d> tailed messages from <%s>.",
              categoryHandled.c_str(), (int) messages.size(), filename.c_str());
          g_Handler->incCounter(categoryHandled, "tail bad", messages.size());
        }
        messages.clear();
      }
    } else if (in.eof()) {
      in.clear();
    }
  }
  LOG_OPER("[%s] Closing tailed log file <%s>",
    categoryHandled.c_str(), filename.c_str());
  in.pop();
}
