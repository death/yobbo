;;;; +----------------------------------------------------------------+
;;;; | YOBBO job scheduler                                            |
;;;; +----------------------------------------------------------------+

;;; System definition

;;; -*- Mode: LISP; Syntax: COMMON-LISP; Package: CL-USER; Base: 10 -*-

(asdf:defsystem #:yobbo
  :description "A small job scheduler hack"
  :author "death <github.com/death>"
  :license "MIT"
  :depends-on (#:alexandria #:bordeaux-threads #:lparallel #:constantia)
  :components
  ((:file "yobbo")))
