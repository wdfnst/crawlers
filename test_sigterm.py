import threading
import time
import signal
import os
import sys

class Watcher:
	def __init__(self):
		self.child = os.fork()
		if self.child == 0:
			return
		else:
			self.watch()

	def watch(self):
		try:
			os.wait()
		except KeyboardInterrupt:
			print '\n'
			print 'KeyBoardInterrupt,begin to clear...'
			#time.sleep(5)
			print '\n'
			self.kill()
		sys.exit()

	def kill(self):
		try:
			print 'clear finish and exit...'
			os.kill(self.child, signal.SIGINT)
		except OSError: pass

class Test(threading.Thread):

	def __init__(self, threadname):
		threading.Thread.__init__(self,name=threadname)
		signal.signal(signal.SIGINT, self.sigint_handle)
		self.running = True

	def sigint_handle(self, signum, frame):
		print "get sigterm signal"
		self.running = False

	def run(self):
		while self.running:
			print "1"
			time.sleep(1)
			print "2"
			time.sleep(1)
			print "3"
			time.sleep(1)
			print "4"
			time.sleep(1)
			print "5"
			time.sleep(1)


def main(script, flag='with'):
	if flag == 'with':
		Watcher()
	elif flag != 'without':
		print 'unrecognized flag: ' + flag
		sys.exit()

	tt = Test('thread - 1')
	tt.start()
	tt.join()
	tt.stop()

if __name__ == '__main__':
	main(*sys.argv)
