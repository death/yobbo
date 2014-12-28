;;;; +----------------------------------------------------------------+
;;;; | YOBBO job scheduler                                            |
;;;; +----------------------------------------------------------------+

(defpackage #:yobbo
  (:import-from #:alexandria #:removef)
  (:use #:cl)
  (:export
   #:job
   #:job-name
   #:job-function
   #:job-schedule
   #:job-last-run
   #:job-active-p
   #:job-next-run
   #:scheduler
   #:whine
   #:command))

(in-package #:yobbo)

;;; Job

(defclass job ()
  ((name :initarg :name :accessor job-name)
   (function :initarg :function :accessor job-function)
   (schedule :initarg :schedule :accessor job-schedule)
   (last-run :initarg :last-run :accessor job-last-run)
   (active :initarg :active :accessor job-active-p))
  (:default-initargs :last-run nil :active t))

(defmethod print-object ((job job) stream)
  (print-unreadable-object (job stream :type t)
    (format stream "~S" (job-name job)))
  job)

(defmethod job-next-run ((job job))
  (let ((schedule (job-schedule job))
        (last-run (job-last-run job))
        (active (job-active-p job)))
    (cond ((or (not active)
               (eq schedule :never))
           nil)
          ((and (numberp schedule)
                (or (null last-run)
                    (< last-run schedule)))
           schedule)
          ((and (consp schedule)
                (eq (car schedule) :every))
           (if (null last-run)
               (get-universal-time)
               (+ last-run (cadr schedule))))
          (t nil))))

;;; Scheduler

(defclass scheduler ()
  ((command-queue :initform (lq:make-queue) :accessor command-queue)
   (jobs :initform '() :accessor jobs)
   (job-threads :initform (make-hash-table) :accessor job-threads)))

(defgeneric whine (scheduler control-string &rest args))
(defgeneric command (scheduler op &rest args))
(defgeneric job-thread (job scheduler))
(defgeneric (setf job-thread) (new-value job scheduler))
(defgeneric run-job (job scheduler))
(defgeneric cancel-job (job scheduler))
(defgeneric start-job (job scheduler))
(defgeneric call-job (job scheduler))
(defgeneric scan-jobs (scheduler))
(defgeneric scheduler-loop (scheduler))
(defgeneric process-command (scheduler op &key))

(defmethod whine ((scheduler scheduler) control-string &rest args)
  (declare (ignore control-string args)))

(defmethod command ((scheduler scheduler) op &rest args)
  (lq:push-queue (cons op args) (command-queue scheduler)))

(defmethod job-thread (job (scheduler scheduler))
  (gethash job (job-threads scheduler)))

(defmethod (setf job-thread) (new-value job (scheduler scheduler))
  (setf (gethash job (job-threads scheduler)) new-value))

(defmethod run-job (job (scheduler scheduler))
  (let ((thread (job-thread job scheduler)))
    (when (or (null thread)
              (not (bt:thread-alive-p thread)))
      (start-job job scheduler))))

(define-condition cancel () ())

(defmethod cancel-job (job (scheduler scheduler))
  (let ((thread (job-thread job scheduler)))
    (cond ((null thread))
          (t
           (when (bt:thread-alive-p thread)
             (bt:interrupt-thread thread (lambda () (signal 'cancel))))
           (setf (job-thread job scheduler) nil)
           (setf (job-last-run job) (get-universal-time))))))

(defmethod start-job (job (scheduler scheduler))
  (setf (job-thread job scheduler)
        (bt:make-thread (lambda () (call-job job scheduler))
                        :name (format nil "Job ~S" (job-name job)))))

(defmethod call-job (job (scheduler scheduler))
  (flet ((command (op &rest args)
           (apply #'command scheduler op args)))
    (handler-case
        (handler-bind ((warning
                        (lambda (condition)
                          (command :job-warn :job job :condition condition))))
          (command :job-started :job job)
          (funcall (job-function job))
          (command :job-done :job job))
      (error (condition)
        (command :job-error :job job :condition condition))
      (cancel (condition)
        (command :job-error :job job :condition condition)))))

(defmethod scan-jobs ((scheduler scheduler))
  (dolist (job (copy-list (jobs scheduler)))
    (let ((thread (job-thread job scheduler)))
      (cond ((null thread)
             (let ((next-run (job-next-run job)))
               (when (and next-run
                          (>= (get-universal-time) next-run))
                 (start-job job scheduler))))
            ((bt:thread-alive-p thread))
            (t (whine scheduler "Dead thread for job ~S" (job-name job))
               (cancel-job job scheduler))))))

(defmethod scheduler-loop ((scheduler scheduler))
  (loop for command = (lq:try-pop-queue (command-queue scheduler)
                                        :timeout 1)
        do (handler-case
               (if (null command)
                   (scan-jobs scheduler)
                   (apply #'process-command scheduler (first command) (rest command)))
             (error (condition)
               (whine scheduler "Error in scheduler loop: ~A (command ~S)"
                      condition command))
             (cancel ()
               (whine scheduler "Scheduler is dying")
               (return-from scheduler-loop)))))

(defmacro define-scheduler-command (name (&rest arglist) &body forms)
  `(defmethod process-command ((scheduler scheduler) (op (eql ',name)) &key ,@arglist)
     (declare (ignorable ,@arglist))
     (flet ((find-job (name)
              (or (find name (jobs scheduler) :key #'job-name)
                  (error "Can't find job with name ~S" name))))
       ,@forms)))

(define-scheduler-command :add (job)
  (push job (jobs scheduler)))

(define-scheduler-command :remove (job-name)
  (removef (jobs scheduler) job-name :count 1 :key #'job-name))

(define-scheduler-command :list (callback)
  (funcall callback (jobs scheduler)))

(define-scheduler-command :activate (job-name)
  (setf (job-active-p (find-job job-name)) t))

(define-scheduler-command :deactivate (job-name)
  (setf (job-active-p (find-job job-name)) nil))

(define-scheduler-command :run (job-name)
  (run-job (find-job job-name) scheduler))

(define-scheduler-command :cancel (job-name)
  (cancel-job (find-job job-name) scheduler))

(define-scheduler-command :die ()
  (signal 'cancel))

(define-scheduler-command :job-started (job)
  (whine scheduler "Job ~A started" (job-name job)))

(define-scheduler-command :job-done (job)
  (cancel-job job scheduler)
  (whine scheduler "Job ~A completed successfully" (job-name job)))

(define-scheduler-command :job-error (job condition)
  (cancel-job job scheduler)
  (whine scheduler "Job ~A failed: ~A" (job-name job) condition))

(define-scheduler-command :job-warn (job condition)
  (whine scheduler "Job ~A warned: ~A" (job-name job) condition))

(define-scheduler-command :job-progress (job condition))
