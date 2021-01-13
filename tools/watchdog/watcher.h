/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 2020-12-11
 * Author: qinyi
 */

#ifndef TOOLS_WATCHDOG_WATCHER_H_
#define TOOLS_WATCHDOG_WATCHER_H_

#include <memory>

#include "tools/watchdog/watchdog_common.h"
namespace Curve {
namespace WatchDog {
class Watcher {
 public:
    Watcher(WatcherName watcherName, const WatchConf* config,
            std::shared_ptr<ServerInfo> serverInfo)
        : watcherName_(watcherName), config_(config), serverInfo_(serverInfo) {}
    virtual int Run() = 0;

 protected:
    const WatcherName watcherName_;
    const WatchConf* config_;
    const std::shared_ptr<ServerInfo> serverInfo_;
};

class ThreadWatcher : public Watcher {
 public:
    ThreadWatcher(WatcherName watcherName, const WatchConf* config,
                  std::shared_ptr<ServerInfo> serverInfo)
        : Watcher(watcherName, config, serverInfo) {}
    int Run();
};

class DiskSmartWatcher : public Watcher {
 public:
    DiskSmartWatcher(WatcherName watcherName, const WatchConf* config,
                     std::shared_ptr<ServerInfo> serverInfo)
        : Watcher(watcherName, config, serverInfo) {}
    int Run();
};

class FileOpWatcher : public Watcher {
 public:
    FileOpWatcher(WatcherName watcherName, const WatchConf* config,
                  std::shared_ptr<ServerInfo> serverInfo)
        : Watcher(watcherName, config, serverInfo) {}
    int Run();

 private:
    int WriteData(int fd, const char* buf, uint64_t offset, int length);
    int ReadData(int fd, char* buf, uint64_t offset, int length);
};

}  // namespace WatchDog
}  // namespace Curve

#endif  // TOOLS_WATCHDOG_WATCHER_H_
