(use gauche.test)
(use util.queue)

(define-syntax test**
  (syntax-rules ()
    [(_ name expected form ...)
     (test* name expected (begin form ...))]))

(test-start "control.actor")
(use control.actor)
(test-module 'control.actor)

(test** "make-actor" <actor>
  (class-of (make-actor (^[] (undefined)))))

(test** "actor?" #t
  (actor? (make-actor (^[] (undefined)))))

(test** "actor-name" #f
  (actor-name (make-actor (^[] (undefined)))))

(test** "actor-name" "foo"
  (actor-name (make-actor (^[] (undefined)) "foo")))

(test** "actor-name-set!" "foo"
  (let1 actor (make-actor (^[] (undefined)))
    (actor-name-set! actor "foo")
    (actor-name actor)))

(test** "actor-specific" #f
  (actor-specific (make-actor (^[] (undefined)))))

(test** "actor-specific-set!" "foo"
  (let1 actor (make-actor (^[] (undefined)))
    (actor-specific-set! actor "foo")
    (actor-specific actor)))

(test** "actor-state" 'new
  (actor-state (make-actor (^[] (undefined)))))

(test** "actor-start!" #t
  (actor? (actor-start! (make-actor (^[] (undefined))))))

(test** "actor-start!" #t
  (string=? "done" (actor-join! (actor-start! (make-actor (^[] "done"))))))

(test** "actor-start!" 'done
  (let1 actor (make-actor (^[] (undefined)))
    (actor-join! (actor-start! actor))
    (actor-state actor)))

(test** "current-actor" #t
  (let1 actor (make-actor (^[] (current-actor)))
    (eq? actor (actor-join! (actor-start! actor)))))

(test** "actor-join!" #t
  (string=? "done" (actor-join! (actor-start! (make-actor (^[] "done"))))))

(test** "actor-join!" #t
  (let* ([actor1 (make-actor (^[] (undefined)))]
         [actor2 (make-actor (^[] (actor-join! actor1) #t))])
    (actor-start! actor2)
    (actor-start! actor1)
    (actor-join! actor2)))

(test** "actor-join!" #t
  (let* ([num-actors 3]
         [actor (make-actor (^[] (undefined)))]
         [actors (map (^[n] (make-actor (^[] (actor-join! actor) n)))
                      (iota num-actors 1))])
    (for-each actor-start! actors)
    (actor-start! actor)
    (let1 res (map actor-join! actors)
      (and (eq? 'done (actor-state actor))
           (every (^a (eq? 'done (actor-state a))) actors)
           (= (/ (* num-actors (+ num-actors 1)) 2)
              (apply + res))))))

(test** "actor-join!" (test-error <error>)
  (let1 actor (actor-start! (make-actor (^[] (error ""))))
    (actor-join! actor)))

(test** "actor-slice!" #t
  (let* ([num-actors 100]
         [n 100]
         [actors (map (^_ (make-actor
                           (^[] (let loop ([nums (iota n 1)] [acc 0])
                                  (actor-slice!)
                                  (if (null? nums)
                                    acc
                                    (loop (cdr nums) (+ acc (car nums))))))))
                      (iota num-actors))])
    (for-each actor-start! actors)
    (let1 res (map actor-join! actors)
      (and (every (^a (eq? 'done (actor-state a))) actors)
           (= (* (/ (* n (+ n 1)) 2) num-actors)
              (apply + res))))))

(test** "actor-yield!" #t
  (let1 actor (actor-start! (make-actor (^[] (actor-yield!))))
    (until (eq? 'yielding (actor-state actor)) (sys-nanosleep #e5e8))
    (actor-resume! actor)
    #t))

(test** "actor-resume!" #t
  (let1 actor (actor-start! (make-actor (^[] (actor-yield!))))
    (until (eq? 'yielding (actor-state actor)) (sys-nanosleep #e5e8))
    (actor-resume! actor)
    (until (eq? 'done (actor-state actor)) (sys-nanosleep #e5e8))
    #t))

(test** "actor-send!" #t
  (let* ([dst (actor-start! (make-actor (^[] (values->list (actor-receive!)))))]
         [src (make-actor (^[] (actor-send! dst "foo")))])
    (string=? "foo" (actor-join! (actor-start! src)))))

(test** "actor-receive!" #t
  (let* ([dst (actor-start! (make-actor (^[] (values->list (actor-receive!)))))]
         [src (make-actor (^[] (actor-send! dst "foo")))])
    (until (eq? 'receiving (actor-state dst)) (sys-nanosleep #e5e8))
    (actor-start! src)
    (let1 res (actor-join! dst)
      (and (string=? "foo" (~ res 0))
           (eq? src (~ res 1))))))

(test** "actor-send!/actor-receive! complete graph ping/pong" #t
  (let ([q (make-mtqueue)]
        [num-actors 100])
    (letrec ([actors (map (^n (make-actor
                               (^[]
                                 (dolist [dst actors] (actor-send! dst 'ping))
                                 (dotimes [_ (* 2 num-actors)]
                                   (receive (val src) (actor-receive!)
                                     (case val
                                       [(ping) (actor-send! src 'pong)]
                                       [(pong) (enqueue! q val)])))
                                 n)))
                          (iota num-actors 1))])
      (for-each actor-start! actors)
      (let1 res (map actor-join! actors)
        (and (every (^a (eq? 'done (actor-state a))) actors)
             (= (expt num-actors 2) (queue-length q))
             (= (/ (* num-actors (+ num-actors 1)) 2)
                (apply + res)))))))

(test** "actor-kill! new" (test-error <actor-error>)
  (let* ([actor1 (make-actor (^[] (undefined)))]
         [actor2 (make-actor (^[] (actor-join! actor1)))])
    (actor-start! actor2)
    (actor-kill! actor1)
    (and (eq? 'killed (actor-state actor1))
         (actor-join! actor2))))

(test** "actor-kill! yielding" (test-error <actor-error>)
  (let* ([actor1 (make-actor (^[] (actor-yield!)))]
         [actor2 (make-actor (^[] (actor-join! actor1)))])
    (actor-start! actor1)
    (actor-start! actor2)
    (actor-kill! actor1)
    (and (eq? 'killed (actor-state actor1))
         (actor-join! actor2))))

(test** "actor-kill! receiving" (test-error <actor-error>)
  (let* ([actor1 (make-actor (^[] (actor-receive!)))]
         [actor2 (make-actor (^[] (actor-join! actor1)))])
    (actor-start! actor1)
    (actor-start! actor2)
    (actor-kill! actor1)
    (and (eq? 'killed (actor-state actor1))
         (actor-join! actor2))))

(test** "actor-kill! done" (test-error <actor-error>)
  (let1 actor (actor-start! (make-actor (^[] (undefined))))
    (until (eq? 'done (actor-state actor)) (sys-nanosleep #e5e8))
    (actor-kill! actor)))

(test** "actor-kill! error" (test-error <actor-error>)
  (let1 actor (actor-start! (make-actor (^[] (error (undefined)))))
    (until (eq? 'error (actor-state actor)) (sys-nanosleep #e5e8))
    (actor-kill! actor)))

(test** "actor-kill! killed" (test-error <actor-error>)
  (let1 actor (make-actor (^[] (undefined)))
    (actor-kill! actor)
    (actor-kill! actor)))

(test-end :exit-on-failure #t)
