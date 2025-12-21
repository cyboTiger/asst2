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
    initiated = false;
    finished = false;
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

        bool is_initiated = false;
        bool is_finished = false;

        sysinfo_mtx_->lock();
        is_initiated = initiated;
        is_finished = finished;
        sysinfo_mtx_->unlock();

        if(!is_initiated || is_finished) continue;

        int curr_task_id;

        sysinfo_mtx_->lock();
        is_finished = finished;
        sysinfo_mtx_->unlock();

        while (!is_finished) {

            sysinfo_mtx_->lock();
            curr_task_id = curr_task_id_;

            if(runnable_ == nullptr || end_ == true) {
                sysinfo_mtx_->unlock();
                break;
            }
            if(curr_task_id >= num_total_tasks_) { 
                sysinfo_mtx_->unlock();
                continue;
            }

            curr_task_id_++;
            sysinfo_mtx_->unlock();

            runnable_->runTask(curr_task_id, num_total_tasks_);

            sysinfo_mtx_->lock();
            num_finished_tasks_++;
            is_finished = finished;
            sysinfo_mtx_->unlock();
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
    initiated = true;
    finished = false;
    sysinfo_mtx_->unlock();

    
    while (num_finished_tasks < num_total_tasks) {
        sysinfo_mtx_->lock();
        num_finished_tasks = num_finished_tasks_;
        sysinfo_mtx_->unlock();
    }

    sysinfo_mtx_->lock();
    finished = true;
    initiated = false;

    num_total_tasks_ = 0;
    curr_task_id_ = 0;
    num_finished_tasks_ = 0;
    runnable_ = nullptr;
    sysinfo_mtx_->unlock();
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    num_threads_ = num_threads;
    thread_pool_ = new std::thread[num_threads];
    mtx_ = new std::mutex();
    cv_ = new std::condition_variable();
    wakeup_mtx_ = new std::mutex();

    runnable_ = nullptr;
    curr_task_id_ = 0;
    num_finished_tasks_ = 0;
    num_total_tasks_ = 0;
    initiated = false;
    finished = false;
    end_ = false;

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    end_ = true;
    for(int i = 0 ; i < num_threads_ ; i++) {
        thread_pool_[i].join();
    }

    delete[] thread_pool_;
    delete mtx_;
    delete cv_;
    delete wakeup_mtx_;
}

void TaskSystemParallelThreadPoolSleeping::sleepingThread() {
    bool end = false;
    mtx_->lock();
    end = end_;
    if(end) {
        mtx_->unlock();
        return;
    }
    mtx_->unlock();

    while (!end) {
        // inspect system task remaining and fetch task id
        mtx_->lock();
        int curr_task_id = curr_task_id_;
        IRunnable* runnable = runnable_;

        std::unique_lock<std::mutex> lk(*wakeup_mtx_);
        if(!initiated || finished || curr_task_id >= num_total_tasks_) {
            mtx_->unlock();

            lk.unlock();
            cv_->notify_one();
            cv_->wait(lk, [this] {
                    mtx_->lock();
                    bool cond = (initiated && !finished && curr_task_id_ < num_finished_tasks_) || end_;
                    mtx_->unlock();
                    return cond;
                }
            );
        }

        mtx_->lock();
        curr_task_id = curr_task_id_;
        if(curr_task_id < num_finished_tasks_) curr_task_id_++;
        mtx_->unlock();
        // end inspection

        // run task
        if(curr_task_id < num_finished_tasks_) runnable->runTask(curr_task_id, num_total_tasks_);
        mtx_->lock();
        if(curr_task_id < num_finished_tasks_) num_finished_tasks_++;
        end = end_;
        mtx_->unlock();
        // end running and updating system info
    }
    

}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    mtx_->lock();

    runnable_ = runnable;
    num_total_tasks_ = num_total_tasks;
    curr_task_id_ = 0;
    num_finished_tasks_ = 0;
    initiated = true;
    finished = false;

    mtx_->unlock();

    cv_->notify_all();

    mtx_->lock();

    while(num_finished_tasks_ < num_total_tasks_) {
        mtx_->unlock();
        std::unique_lock<std::mutex> lk(*wakeup_mtx_);
        cv_->wait(lk, [this] {return num_finished_tasks_ == num_total_tasks_;});

        mtx_->lock();
    }

    finished = true;
    initiated = false;
    mtx_->unlock();
    
    mtx_->lock();


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
