Getting Started
===============

 1. `git submodule init`
 1. `git submodule update`
 1. `vagrant up`
 1. `vagrant ssh`
 1. `echo "awesome" > test`
 1. `swift upload mycontainer test`
 1. `swift download mycontainer test -o -`
 1. `for p in 10 11 20 21 30 31 40 41; do kineticc -P 80${p} list objects; done`

Starting just the Simulator
==========================

 1. `git submodule init`
 1. `git submodule update`
 1. `SIMULATOR_ONLY=true vagrant up`
