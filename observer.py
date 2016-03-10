from threading import Condition

class Observer:
    
    def __init__(self, watchMsg):
        self.watch = watchMsg
        self.status = 0
        self.condition = Condition()

    def notify(self, *args):
        if (self.watch in args[2]):
            self.condition.acquire()
            self.status = 1
            self.condition.notify()
            self.condition.release()

    def waitFor(self, num):
        doneP = 0
        while (doneP < num):
            self.condition.acquire()
            self.condition.wait()
            doneP += 1
            self.condition.release()
            
