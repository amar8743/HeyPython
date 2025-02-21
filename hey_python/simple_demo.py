import sys
import traceback

def greet(name=None):
    try:
        if not name:
            return 'Hello World!'
        return 'Hey {}!'.format(str(name))
    except Exception as e:
        print(traceback.format_exc(), file=sys.stderr)
        logToFile(traceback.format_exc())

def logToFile(str):
    f = open("/tmp/simple_demo_log.txt", "a")
    f.write(str + '\n')
    f.close()