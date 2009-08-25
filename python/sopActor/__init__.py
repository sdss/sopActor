import Queue as _Queue

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
#
# Queue names; use classes so that the unique IDs are automatically generated
#
class MASTER(): pass
class FFS(): pass                       # Flat Field Screen
class FF_LAMP(): pass                   # FF lamps
class HGCD_LAMP(): pass                 # HgCd lamps
class NE_LAMP(): pass                   # Ne lamps
class UV_LAMP(): pass                   # uv lamps
class WHT_LAMP(): pass                  # WHT lamps
class BOSS(): pass                      # command the Boss ICC
class GCAMERA(): pass                   # command the gcamera ICC

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class Msg(object):
    # Priorities
    CRITICAL = 0
    HIGH = 2
    MEDIUM = 4
    NORMAL = 6
    
    # Command types; use classes so that the unique IDs are automatically generated
    class EXIT(): pass
    class FFS_MOVE(): pass
    class FFS_COMPLETE(): pass
    class LAMP_ON(): pass
    class LAMP_COMPLETE(): pass
    class STATUS(): pass
    class EXPOSE(): pass
    class EXPOSURE_FINISHED(): pass
    class REPLY(): pass

    def __init__(self, type, cmd, **data):
        self.type = type
        self.cmd = cmd
        self.priority = Msg.NORMAL      # may be overridden by **data
        #
        # convert data[] into attributes
        #
        for k, v in data.items():
            self.__setattr__(k, v)
        self.__data = data.keys()

    def __repr__(self):
        values = []
        for k in self.__data:
            values.append("%s : %s" % (k, self.__getattribute__(k)))

        return "%s, %s: {%s}" % (self.type.__name__, self.cmd, ", ".join(values))

    def __cmp__(self, rhs):
        """Used when sorting the messages in a priority queue"""
        return self.priority - rhs.priority

class Queue(_Queue.PriorityQueue):
    """A queue type that checks that the message is of the desired type"""

    Empty = _Queue.Empty

    def __init__(self, args):
        _Queue.Queue.__init__(self, args)

    def put(self, arg0, *args, **kwds):
        """Put  messaage onto the queue, calling the superclass's put method
Expects a Msg, otherwise tries to construct a Msg from its arguments"""
        
        if isinstance(arg0, Msg):
            msg = arg0
        else:
            msg = Msg(arg0, *args, **kwds)

        _Queue.Queue.put(self, msg)

    def flush(self):
        """flush the queue"""
    
        while True:
            try:
                msg = self.get(timeout=0)
            except Queue.Empty:
                return

__all__ = ["MASTER", "Msg"]
