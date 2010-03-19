//  Copyright (c) 2007-2008 Facebook
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// See accompanying file LICENSE or visit the Scribe site at:
// http://developers.facebook.com/scribe/
//
// @author Travis Crawford <travis@twitter.com>

#ifndef SCRIBE_ZK_CLIENT_H
#define SCRIBE_ZK_CLIENT_H

#ifdef USE_ZOOKEEPER
#include "common.h"

class ZKClient {
 public:
   ZKClient();
   virtual ~ZKClient();

   static void watcher(zhandle_t *zzh, int type, int state,
                       const char *path, void *watcherCtx);
   void connect(std::string& hostPort);
   void registerTask(std::string& pathName);

   zhandle_t *zh; // Zookeeper connection handle.
};

#endif // SCRIBE_ZK_CLIENT_H
#endif // USE_ZOOKEEPER
