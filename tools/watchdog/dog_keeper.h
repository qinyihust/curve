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

#ifndef TOOLS_WATCHDOG_DOG_KEEPER_H_
#define TOOLS_WATCHDOG_DOG_KEEPER_H_

#include <errno.h>
#include <glog/logging.h>
#include <proc/readproc.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "tools/watchdog/watchdog_common.h"
#include "tools/watchdog/watcher.h"

namespace Curve {
namespace WatchDog {

using std::map;
using std::string;
using std::vector;

class Dog {
 public:
    /* create dog thread for server */
    Dog(const WatchConf conf, const proc_t procInfo, ServerType serverType)
        : pthread_(0),
          config_(conf),
          procInfo_(procInfo),
          serverType_(serverType),
          serverInfo_(nullptr),
          health_(DogHealth::DOG_HEALTHY),
          food_(0) {}

    virtual ~Dog() {}

    virtual int InitWatchers() = 0;
    virtual int InitServerInfo() = 0;

    int Init();

    /* eat food after each operation, to prove it's healthy */
    void Eat() {
        if (!IsBad()) food_ = 0;
    }

    /* refresh stock of food, and if food not eaten, mark dog sick */
    void Feed() {
        if (health_ == DogHealth::DOG_IGNORE) return;

        if (food_ == 0) {
            /* food is eaten up, fill it */
            food_ = 1;
        } else {
            SAFE_LOG_ERROR("food not eaten ,dog for server " << serverInfo_->pid
                                                             << " is sick");
            MarkSick();
        }
    }

    bool IsBad() const {
        return health_ == DogHealth::DOG_SICK || health_ == DogHealth::DOG_DEAD;
    }

    void MarkSick() {
        if (health_ == DogHealth::DOG_HEALTHY) health_ = DogHealth::DOG_SICK;
    }

    bool IsDead() const {
        return DogHealth::DOG_DEAD == health_;
    }

    void SetDead() {
        health_ = DogHealth::DOG_DEAD;
    }

 public:
    pthread_t pthread_;  // dog thread
    const WatchConf config_;
    const proc_t procInfo_;
    ServerType serverType_;
    std::shared_ptr<ServerInfo> serverInfo_;
    std::vector<std::shared_ptr<Watcher>> watchers_;

 private:
    /*
     * if error detected, dog will be set health to DOG_SICK
     */
    DogHealth health_;

    /*
     * dog must eat food in cycles, to prove it is fine;
     * otherwise, if it is blocked in any test operation,
     * it will be marked sick, and going to die.
     */
    int food_;
};

class DogKeeper {
 public:
    explicit DogKeeper(const WatchConf conf) : conf_(conf) {}

    int Run();

 private:
    ServerType GetServerType(const proc_t* procInfo);

    /* assign dog thread for each server */
    int AssignDogForServer();

    /*
     * feeding dog and check dog status,
     * if a dog is sick or not ate food within limited time, kill it
     */
    void FeedingDogs();

    /* force kill server which is killed previously but not die */
    void ForceKillServer();

    /* kill dog thread and server process */
    void KillServer(int pid, std::shared_ptr<Dog> dog);

    const WatchConf conf_;
    std::map<int, std::shared_ptr<Dog>> dogs_;
    std::map<int, time_t> killList_;
};  // class DogKeeper

}  // namespace WatchDog
}  // namespace Curve
#endif  // TOOLS_WATCHDOG_DOG_KEEPER_H_
