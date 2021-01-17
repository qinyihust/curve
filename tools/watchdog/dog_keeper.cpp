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

#include "tools/watchdog/dog_keeper.h"

#include <glog/logging.h>
#include <signal.h>
#include <sys/syscall.h>
#include <sys/types.h>

#include <memory>

#include "tools/watchdog/chunkserver_watchdog.h"
#include "tools/watchdog/watchdog_common.h"

namespace Curve {
namespace WatchDog {
static void* Watch(void* currDog) {
    pthread_detach(pthread_self());
    Dog* dog = reinterpret_cast<Dog*>(currDog);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    SAFE_LOG_INFO("dog for server " << dog->serverInfo_->pid << " is running");

    while (true) {
        for (auto iter = dog->watchers_.begin(); iter != dog->watchers_.end();
             ++iter) {
            if (dog->IsBad()) goto DEATH;
            pthread_testcancel();

            dog->Eat();
            if ((*iter)->Run() < 0) dog->MarkSick();

            if (dog->IsBad()) goto DEATH;
            dog->Eat();
        }

        /* keep eating in sleep */
        for (int i = 0; i < dog->config_.scanIntervalSec; i++) {
            sleep(1);
            dog->Eat();
        }

    }

DEATH:
    SAFE_LOG_ERROR("dog for server " << dog->serverInfo_->pid
                                     << " is going to die");

    return nullptr;
}

int Dog::Init() {
    health_ = DogHealth::DOG_HEALTHY;
    food_ = 0;

    /* only place an empty dog for zombie server process*/
    if (procInfo_.state == 'Z') {
        SAFE_LOG_ERROR("server " << procInfo_.tid
                                 << " is zombie, assign empty dog for it");
        health_ = DogHealth::DOG_IGNORE;
        return 0;
    }

    /* some parameters unrecognized, also place empty dog */
    if (InitServerInfo() < 0) {
        SAFE_LOG_ERROR("Parameter unrecognized, ignore server "
                       << procInfo_.tid);
        health_ = DogHealth::DOG_IGNORE;
        return 0;
    }

    InitWatchers();

    if (pthread_create(&pthread_, NULL, Watch, this)) {
        SAFE_LOG_ERROR("failed to create watchdog thread for server "
                       << serverInfo_->pid);
        health_ = DogHealth::DOG_DEAD;
        return -1;
    }
}

int DogKeeper::Run() {
#if 0
    while (true) {
#endif
    for(int i = 0; i < 5000; ++i) {
        AssignDogForServer();
        FeedingDogs();
#if 0
        sleep(conf_.opTimeoutSec);
#endif
	    usleep(10000);
        ForceKillServer();
    }
}

ServerType DogKeeper::GetServerType(const proc_t* procInfo) {
    if (!strncmp(procInfo->cmd, CHUNKSERVER_CMD_NAME,
                 CHUNKSERVER_CMD_LEN_SHORT) &&
        !strncmp(procInfo->cmdline[0], CHUNKSERVER_CMD_NAME,
                 CHUNKSERVER_CMD_LEN_FULL))
        return ServerType::CHUNKSERVER;
    else
        return ServerType::UNKNOWN;
}

int DogKeeper::AssignDogForServer() {
    PROCTAB* proc = openproc(PROC_FILLCOM | PROC_FILLSTATUS);
    int retryTimes = 0;
    while (proc == nullptr) {
        ++retryTimes;
        if (retryTimes > WATCHDOG_MAX_RETRY) {
            SAFE_LOG_ERR_N(WATCHDOG_DUP_LOG_FREQUENCY,
                           "failed to open /proc, skip dog assignment");
            return -1;
        }
        ::sleep(1);
        proc = openproc(PROC_FILLCOM | PROC_FILLSTATUS);
    }

    int ret = 0;
    proc_t procInfo = {0};
    bool dogAssigned = false;
    while (readproc(proc, &procInfo) != nullptr) {
        /* filter server process */
        ServerType serverType = GetServerType(&procInfo);
        if (serverType == ServerType::UNKNOWN) continue;

        /* skip if process is already watched */
        int pid = procInfo.tid;
        if (dogs_.count(pid) > 0) continue;

        /* assign dog for this server */
        if (serverType == ServerType::CHUNKSERVER) {
            std::shared_ptr<ChunkServerWatchDog> dog =
                std::make_shared<ChunkServerWatchDog>(conf_, procInfo,
                                                      serverType);
            if (dog->Init() < 0) {
                SAFE_LOG_ERROR("failed to init dog for server " << pid);
                ret = -1;
                goto OUT;
            }
            dogs_.emplace(pid, dog);
            dogAssigned = true;
        } else {
            SAFE_LOG_ERR_N(
                WATCHDOG_DUP_LOG_FREQUENCY,
                "Unexpected server type "
                    << static_cast<std::underlying_type<ServerType>::type>(
                           serverType)
                    << " for server " << pid << ", ignored");
        }
    }

OUT:
    closeproc(proc);
    if (dogAssigned)
        SAFE_LOG_INFO("New dog assigned, totally " << dogs_.size() << " dogs");
    return  ret;
}

void DogKeeper::FeedingDogs() {
    auto iter = dogs_.begin();
    while (iter != dogs_.end()) {
        std::shared_ptr<Dog> dog = iter->second;

        dog->Feed();

        /* kill sick dogs */
#if 0
        if (dog->IsBad()) {
#endif
            int pid = iter->first;
            KillServer(pid, iter->second);
            iter = dogs_.erase(iter);
            SAFE_LOG_ERROR("server " << pid
                                     << " and its dog is killed, totally "
                                     << dogs_.size() << " dogs");
#if 0
	} else {
            ++iter;
        }
#endif
    }
}

void DogKeeper::KillServer(int pid, std::shared_ptr<Dog> dog) {
#if 0
    /* kill server process and record in kill list */
    kill(pid, SIGTERM);
    time_t killTime;
    time(&killTime);
    killList_.emplace(pid, killTime);
#endif

    /* kill dog thread */
    dog->SetDead();
    pthread_cancel(dog->pthread_);
    return;
}

void DogKeeper::ForceKillServer() {
    for (auto iter = killList_.begin(); iter != killList_.end();) {
        int serverPid = iter->first;
        int killTime = iter->second;
        time_t currTime;
        time(&currTime);
        /* kill timeout, check if server process remains */
        if (difftime(currTime, killTime) > conf_.killTimeoutSec ||
            currTime < killTime) {
            char status = 0;
            int ret = getProcessStatus(serverPid, &status);
            if (ret < 0 || (status > 0 && status != 'Z')) {
                /* server process is not dead, force kill  */
                kill(serverPid, SIGKILL);
                SAFE_LOG_ERROR("Forcelly kill server " << serverPid);
                iter->second = currTime;
            } else {
                /* process is no longer existed or become zombie */
                iter = killList_.erase(iter);
                continue;
            }
        }
        iter++;
    }
}

}  // namespace WatchDog
}  // namespace Curve
