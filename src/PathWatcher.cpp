#include "PathWatcher.h"

#include <boost/filesystem.hpp>

#ifdef HAVE_INOTIFY
#include <sys/inotify.h>
#define EVENT_BUF_LEN        (1024 * (sizeof(event) + 16))
#define FILE_WATCH_EVENTS  (IN_MODIFY | IN_DELETE_SELF | IN_MOVE_SELF)
#define DIR_WATCH_EVENTS   (IN_CREATE | IN_DELETE | IN_DELETE_SELF | IN_MOVE_SELF | IN_MOVED_FROM)
#endif

PathWatcher::PathWatcher() : inotify_file_wd(-1), inotify_dir_wd(-1) {
#ifdef HAVE_INOTIFY
  inotify_fd = inotify_init();
  if (inotify_fd < 0) {
    LOG_OPER("inotify_init failed: %s", strerror(errno));
  }
#endif
}

PathWatcher::~PathWatcher() {
#ifdef HAVE_INOTIFY
  if (inotify_fd >= 0) {
    close(inotify_fd);
  }
#endif
}

// Clear any existing watched file or directory.
void PathWatcher::clearWatches() {
#ifdef HAVE_INOTIFY
  if (inotify_file_wd >= 0) {
    LOG_OPER("Deleting existing file watch");
    inotify_rm_watch(inotify_fd, inotify_file_wd);
    inotify_file_wd = -1;
  }

  if (inotify_dir_wd >= 0) {
    LOG_OPER("Deleting existing directory watch");
    inotify_rm_watch(inotify_fd, inotify_dir_wd);
    inotify_dir_wd = -1;
  }
  watchedFile.clear();
#endif
}

bool PathWatcher::tryWatchFile(const std::string & path) {
#ifdef HAVE_INOTIFY
  clearWatches();
  boost::filesystem::path watchedPath(path);
  inotify_file_wd = inotify_add_watch(inotify_fd, path.c_str(), FILE_WATCH_EVENTS);
  if (inotify_file_wd >= 0) {
    std::string parentDir = watchedPath.parent_path().string();
    inotify_dir_wd = inotify_add_watch(inotify_fd, parentDir.c_str(), DIR_WATCH_EVENTS);
    LOG_OPER("Set inotify watch for file %s with parent directory %s", path.c_str(), parentDir.c_str());
    watchedFile = watchedPath.filename();
    return true;
  }
  return false;
#endif
  return true;
}

bool PathWatcher::tryWatchDirectory(const std::string & path) {
#ifdef HAVE_INOTIFY
  clearWatches();
  LOG_OPER("Attempting to watch path %s", path.c_str());
  inotify_dir_wd = inotify_add_watch(inotify_fd, path.c_str(), DIR_WATCH_EVENTS);
  if (inotify_dir_wd >= 0) {
    LOG_OPER("Watching path %s", path.c_str());
    return true;
  }
  return false;
#endif
  return true;
}

void PathWatcher::waitForEvent(bool & fileEvent, bool & rewatch) {
#ifdef HAVE_INOTIFY
    rewatch = false;
    fileEvent = false;

    char eventBuf[EVENT_BUF_LEN];
    int rv = read(inotify_fd, eventBuf, EVENT_BUF_LEN);
    if (rv < 0) {
        LOG_OPER("Failed to read inotify event: %s", strerror(errno));
        rewatch = true;
        return;
    }
    int i = 0;
    while (i < rv) {
     struct inotify_event *event;
     event = (struct inotify_event *) &eventBuf[i];
     if (inotify_file_wd != -1 && inotify_file_wd == event->wd) {
       // File event
       fileEvent = true;
     } else if (inotify_file_wd != -1 && inotify_dir_wd == event->wd) {
       // Directory event with a watched file
       std::string alteredFile(event->name);
       if (alteredFile == watchedFile) {
         rewatch = true;
       }
     } else if (inotify_dir_wd == event->wd) {
       // Directory event with no existing file
       rewatch = true;
     }
     i += sizeof(inotify_event) + event->len;
    }
#else
    // inotify is not available. Sleep for one second before checking the file.
    rewatch = false;
    fileEvent = true;
    sleep(1);
#endif
}
