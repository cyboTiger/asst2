#include "tasksys.h"
#include <thread>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
    thread_pool_ = new std::thread[num_threads];
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
    delete[] thread_pool_;
}

void TaskSystemParallelSpawn::threadRun(IRunnable* runnable, int num_total_tasks, std::mutex* mutex, int* curr_task) {
    int turn = -1;

    while(turn < num_total_tasks) {
        mutex->lock();
        turn = *curr_task;
        *curr_task += 1;
        mutex->unlock();
        
        if(turn >= num_total_tasks) 
            break;
        
        runnable->runTask(turn, num_total_tasks);
    }

}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::mutex* mutex = new std::mutex();
    int curr_task = 0;

    for (int i = 0; i < num_threads; i++) {
        thread_pool_[i] = std::thread(&TaskSystemParallelSpawn::threadRun, this, runnable, num_total_tasks, mutex, &curr_task);   
    }
    for (int i = 0; i < num_threads; i++) {
        thread_pool_[i].join();
    }
    delete mutex;
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_ = num_threads;
    // state_ = new ThreadState();
    sysinfo_mtx_ = new std::mutex();
    thread_pool_ = new std::thread[num_threads];
    runnable_ = nullptr;
    curr_task_id_ = 0;
    num_finished_tasks_ = 0;
    num_total_tasks_ = 0;
    initiated_ = false;
    finished_ = false;
    end_ = false;

    for(int i = 0 ; i < num_threads ; i++) {
        thread_pool_[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::spinningThread, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {

    end_ = true;
    for (int i = 0 ; i < num_threads_ ; i++) {
        thread_pool_[i].join();
    }

    // delete state_;
    delete sysinfo_mtx_;
    delete[] thread_pool_;
}

void TaskSystemParallelThreadPoolSpinning::spinningThread() {

    while (!end_) {
        int curr_task_id;
        int num_total_tasks;
        IRunnable* runnable = nullptr;
        // obtain task
        sysinfo_mtx_->lock();
        if(!initiated_ || finished_) {
            sysinfo_mtx_->unlock();
            continue;
        }
        curr_task_id = curr_task_id_ < num_total_tasks_ ? curr_task_id_++ : curr_task_id_;
        num_total_tasks = num_total_tasks_;
        runnable = runnable_;
        sysinfo_mtx_->unlock();

        // execution
        if(curr_task_id < num_total_tasks) {
            if(runnable) {
                runnable->runTask(curr_task_id, num_total_tasks);

                sysinfo_mtx_->lock();
                num_finished_tasks_++;
                sysinfo_mtx_->unlock();
            }
        }
    }

}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {

    int num_finished_tasks;

    sysinfo_mtx_->lock();
    num_total_tasks_ = num_total_tasks;
    runnable_ = runnable;
    curr_task_id_ = 0;
    num_finished_tasks_ = 0;
    num_finished_tasks = num_finished_tasks_;
    initiated_ = true;
    finished_ = false;
    sysinfo_mtx_->unlock();

    
    while (num_finished_tasks < num_total_tasks) {
        sysinfo_mtx_->lock();
        num_finished_tasks = num_finished_tasks_;
        sysinfo_mtx_->unlock();
    }

    sysinfo_mtx_->lock();
    finished_ = true;
    initiated_ = false;
    num_total_tasks_ = 0;
    curr_task_id_ = 0;
    num_finished_tasks_ = 0;
    runnable_ = nullptr;
    sysinfo_mtx_->unlock();

}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    num_threads_ = num_threads;
    thread_pool_ = new std::thread[num_threads];
    mtx_ = new std::mutex();
    cv_ = new std::condition_variable();
    
    runnable_ = nullptr;
    curr_task_id_ = 0;
    num_finished_tasks_ = 0;
    num_total_tasks_ = 0;
    initiated = false;
    finished = false;
    end_ = false;
    
    for (int i = 0; i < num_threads_; i++) {
        thread_pool_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::sleepingThread, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::unique_lock<std::mutex> lock(*mtx_);
        end_ = true;
    }
    cv_->notify_all();
    
    for (int i = 0; i < num_threads_; i++) {
        if (thread_pool_[i].joinable()) {
            thread_pool_[i].join();
        }
    }
    
    delete[] thread_pool_;
    delete mtx_;
    delete cv_;
}

void TaskSystemParallelThreadPoolSleeping::sleepingThread() {
    while (true) {
        int task_id = -1;
        
        // obtain task
        {
            std::unique_lock<std::mutex> lock(*mtx_);
            
            // wait for tasks to come
            cv_->wait(lock, [this]() {
                return end_ || (initiated && curr_task_id_ < num_total_tasks_);
            });
            
            if (end_) {
                return;
            }
            
            if (initiated && curr_task_id_ < num_total_tasks_) {
                task_id = curr_task_id_;
                curr_task_id_++;
            } else {
                continue;  // no execution
            }
        }
        
        // execution
        if (task_id >= 0 && runnable_) {
            runnable_->runTask(task_id, num_total_tasks_);
            
            std::unique_lock<std::mutex> lock(*mtx_);
            num_finished_tasks_++;
            
            // finished all task
            if (num_finished_tasks_ == num_total_tasks_) {
                lock.unlock();
                cv_->notify_all();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    // reset
    {
        std::unique_lock<std::mutex> lock(*mtx_);
        runnable_ = runnable;
        num_total_tasks_ = num_total_tasks;
        curr_task_id_ = 0;
        num_finished_tasks_ = 0;
        initiated = true;
        finished = false;
    }
    
    // wake up all workers
    cv_->notify_all();
    
    // wait for workers to finish tasks
    {
        std::unique_lock<std::mutex> lock(*mtx_);
        cv_->wait(lock, [this]() {
            return num_finished_tasks_ == num_total_tasks_;
        });
        
        // reset
        runnable_ = nullptr;
        initiated = false;
        finished = true;
    }
}
TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
