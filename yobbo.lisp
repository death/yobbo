;;;; +----------------------------------------------------------------+
;;;; | YOBBO job scheduler                                            |
;;;; +----------------------------------------------------------------+

(defpackage #:yobbo
  (:import-from #:alexandria #:removef)
  (:import-from #:constantia #:speaker #:fire #:define-event
                #:add-listener #:remove-listener)
  (:use #:cl)
  (:export
   #:job
   #:job-name
   #:job-function
   #:job-schedule
   #:job-active-p
   #:job-started
   #:job-ended
   #:job-error
   #:job-warn
   #:job-cancelled
   #:job-done
   #:object
   #:scheduler
   #:scheduler-status
   #:entries
   #:scheduler-log
   #:format-control
   #:format-arguments
   #:start
   #:command))

(in-package #:yobbo)

;;; Job

(defclass job (speaker)
  ((name :initarg :name :accessor job-name)
   (function :initarg :function :accessor job-function)
   (schedule :initarg :schedule :accessor job-schedule)
   (active :initarg :active :accessor job-active-p))
  (:default-initargs :active t))

(defmethod print-object ((job job) stream)
  (print-unreadable-object (job stream :type t)
    (format stream "~S" (job-name job)))
  job)

(define-event job-started)
(define-event job-ended)
(define-event job-error object)
(define-event job-warn object)
(define-event job-cancelled object)
(define-event job-done object)

(define-condition cancel () ())

(defmethod run ((job job))
  (fire job 'job-started)
  (handler-case
      (handler-bind ((warning
                      (lambda (condition)
                        (fire job 'job-warn :object condition))))
        (funcall (job-function job)))
    (error (condition)
      (fire job 'job-ended)
      (fire job 'job-error :object condition))
    (cancel (condition)
      (fire job 'job-ended)
      (fire job 'job-cancelled :object condition))
    (:no-error (&rest results)
      (fire job 'job-ended)
      (fire job 'job-done :object (copy-list results)))))

(defmethod job-next-run ((job job) prior-start-time)
  (let ((schedule (job-schedule job)))
    (cond ((or (null schedule) (eq schedule :never)
               (not (job-active-p job)))
           nil)
          ((numberp schedule)
           (if (and prior-start-time (>= prior-start-time schedule))
               nil
               schedule))
          ((and (consp schedule) (eq (car schedule) :every))
           (if (null prior-start-time)
               (get-universal-time)
               (+ prior-start-time (cadr schedule))))
          (t nil))))

;;; Scheduler

(defclass entry ()
  ((start-time :initarg :start-time :accessor start-time)
   (end-time :initarg :end-time :accessor end-time)
   (result :initarg :result :accessor result)
   (status :initarg :status :accessor status)
   (thread :initarg :thread :accessor thread)
   (listener :initarg :listener :accessor listener)))

(defclass scheduler (speaker)
  ((command-queue :initform (lq:make-queue) :accessor command-queue)
   (jobs :initform '() :accessor jobs)
   (entries :initform (make-hash-table) :accessor entries)
   (lock :initform (bt:make-lock "Scheduler lock") :accessor lock)))

(define-event scheduler-status entries)
(define-event scheduler-log format-control format-arguments)

(defgeneric start (scheduler))
(defgeneric command (scheduler op &rest args))
(defgeneric whine (scheduler control-string &rest args))
(defgeneric scheduler-loop (scheduler))
(defgeneric process-command (scheduler op &key))
(defgeneric scan-jobs (scheduler))
(defgeneric scheduler-job-event (scheduler job event))

(defmethod start ((scheduler scheduler))
  (bt:make-thread (lambda () (scheduler-loop scheduler))
                  :name "Job scheduler"))

(defmethod whine ((scheduler scheduler) control-string &rest args)
  (fire scheduler 'scheduler-log
        :format-control control-string
        :format-arguments (copy-list args)))

(defmethod command ((scheduler scheduler) op &rest args)
  (lq:push-queue (cons op args) (command-queue scheduler)))

(defmethod scheduler-loop ((scheduler scheduler))
  (loop for command = (lq:try-pop-queue (command-queue scheduler)
                                        :timeout 1)
        do (handler-case
               (bt:with-lock-held ((lock scheduler))
                 (if (null command)
                     (scan-jobs scheduler)
                     (apply #'process-command scheduler (first command) (rest command))))
             (error (condition)
               (whine scheduler "Error in scheduler loop: ~A (command ~S)"
                      condition command))
             (cancel ()
               (whine scheduler "Scheduler is dying")
               (return-from scheduler-loop)))))

(defmethod scan-jobs ((scheduler scheduler))
  (dolist (job (copy-list (jobs scheduler)))
    (let ((entry (gethash job (entries scheduler))))
      (cond ((null (thread entry))
             (let ((next-run (job-next-run job (start-time entry))))
               (when (and next-run
                          (>= (get-universal-time) next-run))
                 (start-job job entry scheduler))))
            ((not (bt:thread-alive-p (thread entry)))
             (whine scheduler "Dead thread for job ~S" job)
             (cancel-job job entry scheduler))))))

(defmethod start-job (job entry (scheduler scheduler))
  (setf (status entry) :starting)
  (setf (thread entry)
        (bt:make-thread (lambda () (run job))
                        :name (format nil "Job ~S" (job-name job)))))

(defmethod cancel-job (job entry (scheduler scheduler))
  (let ((thread (thread entry)))
    (cond ((null thread))
          ((bt:thread-alive-p thread)
           (setf (status entry) :cancelling)
           (bt:interrupt-thread thread (lambda () (signal 'cancel))))
          (t
           (setf (thread entry) nil)))))

(defmethod scheduler-job-event ((scheduler scheduler) job event)
  (declare (ignore job event)))

(defmethod scheduler-job-event :around ((scheduler scheduler) job event)
  (bt:with-lock-held ((lock scheduler))
    (call-next-method)))

(defmethod scheduler-job-event ((scheduler scheduler) job (event job-started))
  (let ((entry (gethash job (entries scheduler))))
    (setf (start-time entry) (get-universal-time))
    (setf (end-time entry) nil)
    (setf (result entry) nil)
    (setf (status entry) :running)))

(defmethod scheduler-job-event ((scheduler scheduler) job (event job-error))
  (let ((entry (gethash job (entries scheduler))))
    (setf (end-time entry) (get-universal-time))
    (setf (result entry) (object event))
    (setf (status entry) :failed)
    (setf (thread entry) nil)))

(defmethod scheduler-job-event ((scheduler scheduler) job (event job-cancelled))
  (let ((entry (gethash job (entries scheduler))))
    (setf (end-time entry) (get-universal-time))
    (setf (result entry) (object event))
    (setf (status entry) :cancelled)
    (setf (thread entry) nil)))

(defmethod scheduler-job-event ((scheduler scheduler) job (event job-done))
  (let ((entry (gethash job (entries scheduler))))
    (setf (end-time entry) (get-universal-time))
    (setf (result entry) nil)
    (setf (status entry) :done)
    (setf (thread entry) nil)))

(defmethod scheduler-job-event ((scheduler scheduler) job (event job-warn))
  (whine scheduler "Job ~S warned: ~A" job (object event)))

(defmacro define-scheduler-command (name (&rest arglist) &body forms)
  `(defmethod process-command ((scheduler scheduler) (op (eql ',name)) &key ,@arglist)
     (declare (ignorable ,@arglist))
     (flet ((find-job (name)
              (or (find name (jobs scheduler) :key #'job-name)
                  (error "Can't find job with name ~S" name))))
       ,@forms)))

(define-scheduler-command :add (job)
  (let ((listener (lambda (event)
                    (scheduler-job-event scheduler job event))))
    (setf (gethash job (entries scheduler))
          (make-instance 'entry
                         :start-time nil
                         :end-time nil
                         :result nil
                         :status :pending
                         :thread nil
                         :listener listener))
    (push job (jobs scheduler))
    (add-listener listener job)))

(define-scheduler-command :remove (job-name)
  (let* ((job (find-job job-name))
         (entry (gethash job (entries scheduler))))
    (remove-listener (listener entry) job)
    (removef (jobs scheduler) job :count 1)
    (remhash job (entries scheduler))))

(define-scheduler-command :request-status ()
  (fire scheduler 'scheduler-status
        :entries (entries scheduler)))

(define-scheduler-command :activate (job-name)
  (setf (job-active-p (find-job job-name)) t))

(define-scheduler-command :deactivate (job-name)
  (setf (job-active-p (find-job job-name)) nil))

(define-scheduler-command :run (job-name)
  (let* ((job (find-job job-name))
         (entry (gethash job (entries scheduler))))
    (when (null (thread entry))
      (start-job job entry scheduler))))

(define-scheduler-command :cancel (job-name)
  (let* ((job (find-job job-name))
         (entry (gethash job (entries scheduler))))
    (cancel-job job entry scheduler)))

(define-scheduler-command :die ()
  (signal 'cancel))
