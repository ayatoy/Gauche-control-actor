(define-module control.actor
  (use gauche.partcont)
  (use gauche.record)
  (use gauche.threads)
  (use util.queue)
  (export <actor-error>
          actor-error?
          <actor-manager>
          actor-manager?
          actor-manager-min
          actor-manager-min-set!
          actor-manager-max
          actor-manager-max-set!
          actor-manager-num-workers
          actor-manager-num-awaits
          actor-manager-add-worker!
          actor-manager-delete-worker!
          current-actor-manager
          <actor>
          actor?
          actor-name
          actor-name-set!
          actor-specific
          actor-specific-set!
          actor-state
          current-actor
          make-actor
          actor-start!
          actor-kill!
          actor-join!
          actor-yield!
          actor-resume!
          actor-slice!
          actor-receive!
          actor-send!
          ))
(select-module control.actor)

;;;; condition

(define-condition-type <actor-error> <error> actor-error?)
(define-condition-type <actor-fault> <error> actor-fault?)

;;;; manager

(define-record-type <actor-manager> %make-actor-manager actor-manager?
  (mutex actor-manager-mutex)
  (min actor-manager-min %actor-manager-min-set!)
  (max actor-manager-max %actor-manager-max-set!)
  (count actor-manager-count actor-manager-count-set!)
  (queue actor-manager-queue)
  (workers actor-manager-workers))

(define-method write-object ((am <actor-manager>) oport)
  (format oport "#<actor-manager ~s ~s ~s>"
          (actor-manager-min am)
          (actor-manager-max am)
          (actor-manager-count am)))

(define (actor-manager-min-set! am n)
  (unless (>= n 0) (error <actor-error> "must be nonnegative integer"))
  (%actor-manager-min-set! am n))

(define (actor-manager-max-set! am n)
  (unless (>= n 0) (error <actor-error> "must be nonnegative integer"))
  (%actor-manager-max-set! am n))

(define (actor-manager-num-workers am)
  (hash-table-num-entries (actor-manager-workers am)))

(define (actor-manager-num-awaits am)
  (actor-manager-count am))

(define (actor-manager-worker-checkin! am w)
  (hash-table-put! (actor-manager-workers am) w #t)
  (actor-manager-count-set! am (+ (actor-manager-count am) 1)))

(define (actor-manager-worker-checkout! am w)
  (hash-table-put! (actor-manager-workers am) w #f)
  (actor-manager-count-set! am (- (actor-manager-count am) 1)))

(define (%actor-manager-add-worker! am)
  (and (> (actor-manager-max am) (actor-manager-num-workers am))
       (begin (actor-manager-worker-checkin!
               am
               (thread-start! (make-thread (cut actor-manager-worker am))))
              #t)))

(define (actor-manager-add-worker! am)
  (let1 mutex (actor-manager-mutex am)
    (mutex-lock! mutex)
    (begin0 (%actor-manager-add-worker! am)
            (mutex-unlock! mutex))))

(define (%actor-manager-delete-worker! am)
  (let1 ws (actor-manager-workers am)
    (and (> (actor-manager-num-workers am) (actor-manager-min am))
         (let loop ([wal (hash-table->alist ws)])
           (cond [(null? wal) #f]
                 [(cdar wal)
                  (let1 t (caar wal)
                    (actor-manager-worker-checkout! am t)
                    (hash-table-delete! ws t)
                    (thread-terminate! t)
                    #t)]
                 [else (loop (cdr wal))])))))

(define (actor-manager-delete-worker! am)
  (let1 mutex (actor-manager-mutex am)
    (mutex-lock! mutex)
    (begin0 (%actor-manager-delete-worker! am)
            (mutex-unlock! mutex))))

(define (actor-manager-reinforce! am)
  (and (not (queue-empty? (actor-manager-queue am)))
       (>= 0 (actor-manager-count am))
       (%actor-manager-add-worker! am)))

(define (actor-manager-worker am)
  (let ([thread (current-thread)]
        [mmutex (actor-manager-mutex am)]
        [queue (actor-manager-queue am)])
    (while #t
      (let* ([actor (dequeue/wait! queue)]
             [amutex (actor-mutex actor)])
        (mutex-lock! mmutex)
        (actor-manager-worker-checkout! am thread)
        (actor-manager-reinforce! am)
        (thread-specific-set! thread actor)
        (mutex-unlock! mmutex)
        (reset
          (mutex-lock! amutex)
          (case (actor-state actor)
            [(new receiving yielding)
             (actor-queuing-set! actor #f)
             (actor-state-set! actor 'running)
             ((begin0 (actor-action actor) (mutex-unlock! amutex)))]
            [(killed)
             (condition-variable-broadcast! (actor-condv actor))
             (mutex-unlock! amutex)]
            [else
             (mutex-unlock! amutex)
             (error <actor-fault> actor-manager-worker actor)]))
        (mutex-lock! mmutex)
        (thread-specific-set! thread #f)
        (actor-manager-worker-checkin! am thread)
        (mutex-unlock! mmutex)))))

(define (make-actor-manager :optional (size 4) (min 1) (max 16))
  (unless (every (^n (>= n 0)) (list size min max))
    (error <actor-error> "must be nonnegative integer"))
  (rlet1 am (%make-actor-manager (make-mutex) min max 0 (make-mtqueue)
                                 (make-hash-table 'eq?))
    (let1 mutex (actor-manager-mutex am)
      (mutex-lock! mutex)
      (dotimes [_ size] (%actor-manager-add-worker! am))
      (mutex-unlock! mutex))))

;;;; actor

(define-record-type <actor> %make-actor actor?
  (mutex actor-mutex)
  (condv actor-condv)
  (name actor-name actor-name-set!)
  (specific actor-specific actor-specific-set!)
  (state actor-state actor-state-set!)
  (queuing actor-queuing actor-queuing-set!)
  (mailbox actor-mailbox)
  (action actor-action actor-action-set!)
  (result actor-result actor-result-set!))

(define-method write-object ((actor <actor>) oport)
  (format oport "#<actor ~s ~s>"
          (actor-name actor)
          (actor-state actor)))

(define (actor-enqueue! actor)
  (unless (actor-queuing actor)
    (enqueue! (actor-manager-queue (current-actor-manager)) actor)
    (actor-queuing-set! actor #t)))

(define (current-actor)
  (let1 specific (thread-specific (current-thread))
    (and (actor? specific) specific)))

(define (finish-action! actor state result)
  (let1 mutex (actor-mutex actor)
    (mutex-lock! mutex)
    (case (actor-state actor)
      [(running)
       (actor-result-set! actor result)
       (actor-action-set! actor #f)
       (actor-state-set! actor state)
       (condition-variable-broadcast! (actor-condv actor))
       (mutex-unlock! mutex)]
      [(killed)
       (condition-variable-broadcast! (actor-condv actor))
       (mutex-unlock! mutex)]
      [else
       (mutex-unlock! mutex)
       (error <actor-fault> finish-action! actor)])))

(define (make-actor thunk :optional (name #f))
  (rlet1 actor (%make-actor (make-mutex) (make-condition-variable)
                            name #f 'new #f (make-queue) #f (undefined))
    (actor-action-set!
     actor
     (^[] (guard (e [else (finish-action! actor 'error e)])
            (finish-action! actor 'done (thunk)))))))

(define (actor-start! actor)
  (let1 mutex (actor-mutex actor)
    (mutex-lock! mutex)
    (case (actor-state actor)
      [(new)
       (actor-enqueue! actor)
       (mutex-unlock! mutex)
       actor]
      [else
       (mutex-unlock! mutex)
       (error <actor-error> "actor is already started:" actor)])))

(define (actor-kill! actor)
  (let1 mutex (actor-mutex actor)
    (mutex-lock! mutex)
    (case (actor-state actor)
      [(running)
       (actor-state-set! actor 'killed)
       (mutex-unlock! mutex)
       actor]
      [(new yielding receiving)
       (actor-state-set! actor 'killed)
       (unless (actor-queuing actor)
         (condition-variable-broadcast! (actor-condv actor)))
       (mutex-unlock! mutex)
       actor]
      [else
       (mutex-unlock! mutex)
       (error <actor-error> "actor is already terminated:" actor)])))

(define (actor-join! actor)
  (let1 mutex (actor-mutex actor)
    (mutex-lock! mutex)
    (case (actor-state actor)
      [(done)
       (mutex-unlock! mutex)
       (actor-result actor)]
      [(error)
       (mutex-unlock! mutex)
       (raise (actor-result actor))]
      [(killed)
       (mutex-unlock! mutex)
       (error <actor-error> "actor is killed:" actor)]
      [else
       (mutex-unlock! mutex (actor-condv actor))
       (actor-join! actor)])))

(define (actor-resume! actor)
  (let1 mutex (actor-mutex actor)
    (mutex-lock! mutex)
    (case (actor-state actor)
      [(yielding)
       (actor-enqueue! actor)
       (mutex-unlock! mutex)
       actor]
      [else
       (mutex-unlock! mutex)
       (error <actor-error> "actor is not yielding:" actor)])))

(define (actor-yield!)
  (if-let1 self (current-actor)
    (let1 mutex (actor-mutex self)
      (mutex-lock! mutex)
      (case (actor-state self)
        [(running)
         (shift reaction
           (actor-action-set! self reaction)
           (actor-state-set! self 'yielding)
           (mutex-unlock! mutex))]
        [(killed)
         (shift reaction
           (condition-variable-broadcast! (actor-condv self))
           (mutex-unlock! mutex))]
        [else
         (mutex-unlock! mutex)
         (error <actor-fault> actor-yield! self)]))
    (error <actor-error> "it does not running within actor")))

(define (actor-slice!)
  (if-let1 self (current-actor)
    (let1 mutex (actor-mutex self)
      (mutex-lock! mutex)
      (case (actor-state self)
        [(running)
         (shift reaction
           (actor-action-set! self reaction)
           (actor-state-set! self 'yielding)
           (actor-enqueue! self)
           (mutex-unlock! mutex))]
        [(killed)
         (shift reaction
           (condition-variable-broadcast! (actor-condv self))
           (mutex-unlock! mutex))]
        [else
         (mutex-unlock! mutex)
         (error <actor-fault> actor-slice! self)]))
    (error <actor-error> "it does not running within actor")))

(define (actor-receive!)
  (if-let1 self (current-actor)
    (let1 mutex (actor-mutex self)
      (mutex-lock! mutex)
      (case (actor-state self)
        [(running)
         (if-let1 msg (dequeue! (actor-mailbox self) #f)
           (begin
             (mutex-unlock! mutex)
             (values (vector-ref msg 0) (vector-ref msg 1)))
           (begin
             (shift reaction
               (actor-action-set! self reaction)
               (actor-state-set! self 'receiving)
               (mutex-unlock! mutex))
             (actor-receive!)))]
        [(killed)
         (shift reaction
           (condition-variable-broadcast! (actor-condv self))
           (mutex-unlock! mutex))]
        [else
         (mutex-unlock! mutex)
         (error <actor-fault> actor-receive! self)]))
    (error <actor-error> "it does not running within actor")))

(define (actor-send! dst val)
  (if-let1 src (current-actor)
    (let1 mutex (actor-mutex dst)
      (mutex-lock! mutex)
      (enqueue! (actor-mailbox dst) (vector val src))
      (when (eq? 'receiving (actor-state dst))
        (actor-enqueue! dst))
      (mutex-unlock! mutex)
      val)
    (error <actor-error> "it does not running within actor")))

;;;; instance

(define current-actor-manager (let1 am (make-actor-manager) (^[] am)))
