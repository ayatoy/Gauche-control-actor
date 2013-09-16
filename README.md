# Partial continuation based light-weight thread like actor model for Gauche

## Features

* With partial continuation
* With pthread pool
* Non-preemptive
* But it have a few slicing point
* Not have dedicated synchronization
* Each thread have a mailbox
* Asynchronous message passing by `actor-send!` and `actor-receive!`

## Install

    $ git clone git://github.com/ayatoy/Gauche-control-actor.git
    $ cd Gauche-control-actor
    $ ./DIST gen
    $ ./configure
    $ make check
    $ [sudo] make install
