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

#ifndef SCRIBE_SOURCE_H_
#define SCRIBE_SOURCE_H_

#include "common.h"
#include "conf.h"
#include "PathWatcher.h"

#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/seek.hpp>
#include <iostream>
#include <sys/stat.h>


class Source {
 public:
  static bool createSource(boost::property_tree::ptree& conf,
    boost::shared_ptr<Source>& newSource);
  Source(boost::property_tree::ptree& conf);
  virtual ~Source();
  virtual void configure();
  virtual void start();
  virtual void stop();
  virtual void run();

 protected:
  boost::property_tree::ptree configuration;
  std::string categoryHandled;
  pthread_t sourceThread;
  bool active;
  bool validConfiguration;
};

class TailSource : public Source {
 public:
  TailSource(boost::property_tree::ptree& configuration);
  ~TailSource();
  void configure();
  void start();
  void stop();
  void run();
 private:
  std::string filename;
  boost::iostreams::filtering_istream in;
  PathWatcher pathWatcher;
  bool watchPath();
};

#endif /* SCRIBE_SOURCE_H_ */
