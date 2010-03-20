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

#include "common.h"
#include "zk_client.h"

using namespace std;
using boost::lexical_cast;

ZKClient::ZKClient() {
  if (debug_level) {
    zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
  }
}

ZKClient::~ZKClient() {
  zookeeper_close(zh);
}

void ZKClient::watcher(zhandle_t *zzh, int type, int state,
                       const char *path, void *watcherCtx) {}

void ZKClient::connect(string& hostPort) {
  zh = zookeeper_init(hostPort.c_str(), watcher, 10000, 0, 0, 0);
}

/*
 * Register this node in Zookeeper,
 * in `/scribe/group/prefix/host:port' format.
 */
bool ZKClient::registerTask(string& pathName, unsigned long int port) {
  static string delimiter = "/";
  static string contents = "";
  size_t index = pathName.find(delimiter, 0);
  char tmp[0];

  // Prefixs are created as regular znodes.
  while (index < string::npos) {
    index = pathName.find(delimiter, index+1);
    string prefix = pathName.substr(0, index);
    zoo_create(zh, prefix.c_str(), contents.c_str(), contents.length(),
               &ZOO_CREATOR_ALL_ACL, 0, tmp, sizeof(tmp));
  }

  // Register this scribe as an ephemeral node.
  char hostname[1024];
  hostname[1023] = '\0';
  gethostname(hostname, 1023);
  string fullPath = pathName + "/" + hostname + ":";
  fullPath += lexical_cast<string>(port);
  int ret = zoo_create(zh, fullPath.c_str(), contents.c_str(),
                       contents.length(), &ZOO_CREATOR_ALL_ACL,
                       ZOO_EPHEMERAL, tmp, sizeof(tmp));
  if (ZOK != ret) {
    return false;
  }
  return true;
}

// Get the best host:port to send messages to at this time.
bool ZKClient::getRemoteScribe(string& parentZnode,
                               string& remoteHost,
                               unsigned long& remotePort) {
  struct String_vector children;
  if (ZOK != zoo_get_children(zh, parentZnode.c_str(), 0, &children)) {
    return false;
  }

  if (0 == children.count) {
    return false;
  }

  // Choose a random scribe.
  srand(time(NULL));
  int remoteScribeIndex = rand() % children.count;

  string remoteScribeZnode = children.data[remoteScribeIndex];
  string delimiter = ":";
  size_t index = remoteScribeZnode.find(delimiter);

  remoteHost = remoteScribeZnode.substr(0, index);
  string port = remoteScribeZnode.substr(index+1, string::npos);
  remotePort = static_cast<unsigned long>(atol(port.c_str()));
  return true;
}
