from threading import Condition
import time

class Observer:
    
    def __init__(self, watchMsg):
        self.watch = watchMsg
        self.condition = Condition()
        self.numNotifications = 0

    def notify(self, host, line):
        #print "notify looks for {}, got {}".format(self.watch, line)
        if (self.watch in line):
            self.condition.acquire()
            self.numNotifications += 1
            #print "Notify"
            self.condition.notify()
            self.condition.release()

    def waitFor(self, num):
        doneP = 0
        doneP += self.numNotifications
        self.numNotifications = 0
        while (doneP < num):
            self.condition.acquire()
            self.condition.wait(1)
            doneP += self.numNotifications
            self.numNotifications = 0
            self.condition.release()
            
