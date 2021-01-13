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

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iostream>
#include <string>

#include "tools/watchdog/chunkserver_watchdog.h"

namespace Curve {
namespace WatchDog {

using std::string;

static bool ValidateTime(const char* flagname, uint32_t time) {
    if (time == 0 || time > WATCHDOG_MAX_TIME_INTV) {
        std::cerr << flagname << " is invalid: " << time
                  << ", must in range [1, " << WATCHDOG_MAX_TIME_INTV << "]"
                  << std::endl;
        return false;
    } else {
        return true;
    }
}

DEFINE_uint32(scanCycle, WATCHDOG_DEF_SCAN_CYCLE, "time interval to scan disk");
DEFINE_validator(scanCycle, &ValidateTime);

DEFINE_uint32(opTimeout, WATCHDOG_DEF_OP_TIMEOUT,
              "max time for a filesytem operation");
DEFINE_validator(opTimeout, &ValidateTime);

DEFINE_uint32(killTimeoutSec, WATCHDOG_DEF_KILL_TIMEOUT,
              "max time for killing chunkserver");
DEFINE_validator(killTimeoutSec, &ValidateTime);

const char kWatchDogUsage[] =
    "Usage: watchdog [OPTIONS...]\n"
    "must to run with root privilege.\n"
    "OPTIONS:\n"
    "-scanCycleSec : time interval to scan disk\n"
    "-opTimeoutSec : max time for a certain operation\n"
    "-killTimeoutSec : max time for killing chunkserver\n"
    "-logPath : log path for chunkserver_watchdog\n";

static void print_config() {
    LOG(INFO) << "chunkserver watchdog is starting...";
    LOG(INFO) << "scanCycle=" << FLAGS_scanCycle;
    LOG(INFO) << "opTimeout=" << FLAGS_opTimeout;
    LOG(INFO) << "killTimeoutSec=" << FLAGS_killTimeoutSec;
    LOG(INFO) << "logDir=" << FLAGS_log_dir;
}

int ChunkServerWatchDogRun() {
    print_config();
    WatchConf conf = {Curve::WatchDog::FLAGS_scanCycle,
                      Curve::WatchDog::FLAGS_opTimeout,
                      Curve::WatchDog::FLAGS_killTimeoutSec};
    DogKeeper dogKeeper(conf);
    dogKeeper.Run();

    google::ShutdownGoogleLogging();

    return 0;
}

}  // namespace WatchDog
}  // namespace Curve

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    gflags::SetVersionString("0.1");
    gflags::SetUsageMessage(Curve::WatchDog::kWatchDogUsage);

    /* init logging */
    FLAGS_minloglevel = 0;
    if (FLAGS_log_dir.empty()) FLAGS_log_dir = WATCHDOG_DEF_LOG_PATH;
    if (Curve::WatchDog::initLogPath() < 0) return -1;
    google::InitGoogleLogging(argv[0]);

    int ret = 0;
    int pidFd = -1;
    if (Curve::WatchDog::checkEnv() < 0) {
        ret = -1;
        goto QUIT;
    }

    /* lock pid file to prevent multiple instances */
    pidFd = Curve::WatchDog::lockPidFile();
    if (pidFd < 0) {
        ret = -1;
        goto QUIT;
    }

    ret = Curve::WatchDog::ChunkServerWatchDogRun();
    close(pidFd);

QUIT:
    google::ShutdownGoogleLogging();
    return ret;
}
