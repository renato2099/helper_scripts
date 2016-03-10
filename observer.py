class Observer:
    
    def __init__(self, watchMsg):
        self.watch = watchMsg
        self.status = 0
        #TODO add semaphore

    def notify(self, *args):
        if (self.watch in args[2]):
            print "Found what needed"
            self.status = 1
            #TODO release semaphore
            
