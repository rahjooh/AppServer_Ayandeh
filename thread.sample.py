import threading
import time

queue_list = []
eject_list = []
class ThreadingExample(object):
    """ Threading example class
    The run() method will be started and it will run in the background
    until the application exits.
    """

    def __init__(self, interval=1):
        """ Constructor
        :type interval: int
        :param interval: Check interval, in seconds
        """
        self.interval = interval

        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True                            # Daemonize thread
        thread.start()                                  # Start the execution

    def run(self):
        global queue_list
        for item in queue_list:
            time.sleep(item)
            eject_list.append(item)
            queue_list.remove(item)
            time.sleep(self.interval)


example = ThreadingExample()
for x in xrange(1,100):
    queue_list.append(x)

while (queue_list.count() > 0) :
    for available in eject_list:
                print str(available) + " is recived"
