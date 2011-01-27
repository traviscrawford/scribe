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
 * @author John Corwin <jcorwin@twitter.com>
 */

#ifndef PATH_WATCHER_H

#include "common.h"

class PathWatcher {
 public:
   PathWatcher();
   ~PathWatcher();

   // Attempt to watch a file. Return true if successful.
   bool tryWatchFile(const std::string & path);

   // Attempt to watch a directory. Return true if successful.
   bool tryWatchDirectory(const std::string & path);

   /**
    * Wait for events.
    * fileEvent will be set if the watched file was modified.
    * rewatch will be set if a change to the file or parent directory
    *  requires rewatching.
    */
   void waitForEvent(bool & fileEvent, bool & rewatch);

 private:
  void clearWatches();
  int inotify_fd;
  int inotify_file_wd;
  int inotify_dir_wd;
  std::string watchedFile;
};

#endif
