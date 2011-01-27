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
}

TailSource::~TailSource() {
}

void TailSource::configure() {
  Source::configure();
  filename = configuration.get<string>("file", "");
  if (filename == "") {
    LOG_OPER("[%s] Invalid TailSource configuration! No <file> specified.",
      categoryHandled.c_str());
    validConfiguration = false;
  } else {
    if (!watchPath()) {
      validConfiguration = false;
    }
  }
}

bool TailSource::watchPath() {
  if (pathWatcher.tryWatchFile(filename)) {
    return true;
  }
  // File watch failed. Try watching each parent directory.
  LOG_OPER("Unable to watch %s. Attempting to watch parent directories.", filename.c_str());
  boost::filesystem::path fullPath(filename);
  deque<string> pathStack;
  boost::filesystem::path::iterator pathIter = fullPath.begin();
  while (pathIter != fullPath.end()) {
    pathStack.push_back(*pathIter);
    pathIter++;
  }
  // We already failed to watch the file. Remove it from the path stack.
  pathStack.pop_back();

  // Attempt to watch a parent path of the file.
  while (!pathStack.empty()) {
    boost::filesystem::path pathToWatch;
    for (deque<string>::iterator elem = pathStack.begin(); elem != pathStack.end(); elem++) {
      pathToWatch /= *elem;
    }
    string strPathToWatch = pathToWatch.string();
    LOG_OPER("Attempting to watch path %s", strPathToWatch.c_str());
    if (pathWatcher.tryWatchDirectory(strPathToWatch)) {
      return true;
    }
    pathStack.pop_back();
  }
  LOG_OPER("Failed to watch any parent paths of %s", filename.c_str());
  sleep(10);
  return false;
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
    bool fileEvent;
    bool rewatch;
    pathWatcher.waitForEvent(fileEvent, rewatch);
    if (rewatch) {
      watchPath();
    }
    if (!fileEvent) {
      continue;
    }

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
