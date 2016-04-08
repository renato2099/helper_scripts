from threading import Condition
import time

class Observer:
    
    def __init__(self, watchMsg):
        self.watch = watchMsg
        self.condition = Condition()
        self.numNotifications = 0

    def notify(self, *args):
        if (self.watch in args[2]):
            self.condition.acquire()
            self.numNotifications += 1
            print "Notify"
            self.condition.notify()
            self.condition.release()

    def waitFor(self, num):
        doneP = 0
        while (doneP < num):
            self.condition.acquire()
            self.condition.wait()
            doneP += self.numNotifications
            self.numNotifications = 0
            print "Notified {0}/{1}".format(doneP, num)
            self.condition.release()
            
