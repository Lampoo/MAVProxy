import sys, os, time, signal, multiprocessing
from optparse import OptionParser
import gi
gi.require_version("Gst", '1.0')
from gi.repository import Gst, GLib
Gst.init(None)


class GstPipeline(object):
    def __init__(self):
        self.pipeline = None

    def source(self):
        return self.pipeline.get_by_name('source')

    def tee(self):
        return self.pipeline.get_by_name('tee')

    def sink(self):
        return self.pipeline.get_by_name('sink')

    def start(self):
        bus = self.pipeline.get_bus()
        bus.enable_sync_message_emission()
        bus.set_sync_handler(self.bus_message_sync_handler, self)
        self.pipeline.set_state(Gst.State.PLAYING)

    def stop(self):
        bus = self.pipeline.get_bus()
        bus.disable_sync_message_emission()
        self.pipeline.set_state(Gst.State.NULL)

    def bus_message_sync_handler(self, bus, message, userdata):
        if message.type == Gst.MessageType.ERROR:
            err, debug = message.parse_error()
            print("Error received from element %s: %s" % (message.src.get_name(), err))
            print("Debug information %s" % debug)
        elif message.type == Gst.MessageType.EOS:
            print("End-of-stream")
        elif message.type == Gst.MessageType.STATE_CHANGED:
            if isinstance(message.src, Gst.Pipeline):
                old_state, new_state, pending_state = message.parse_state_changed()
                print("Pipeline state changed from %s to %s." % (old_state.value_nick, new_state.value_nick))
        else:
            print("Unexpected message received.")

    @staticmethod
    def from_uri(uri):
        if uri.startswith('videotestsrc'):
            return TestSource()
        elif uri.startswith('udp://'):
            return UDPSource(uri)
        elif uri.startwith('udp264://'):
            return UDPSource(uri.replace('udp264://', 'udp://'))
        elif uri.startwith('udp265://'):
            return UDP265Source(uri.replace('udp265://', 'udp://'))
        elif uri.startswith('rtsp://'):
            return RTSPSource(uri)
        else:
            return None

    def main_loop(self, exit):
        bus = self.pipeline.get_bus()

        # start pipeline
        self.pipeline.set_state(Gst.State.PLAYING)

        while not exit.is_set():
            message = bus.timed_pop_filtered(10000, Gst.MessageType.ANY)
            if message:
                if message.type == Gst.MessageType.ERROR:
                    err, debug = message.parse_error()
                    print("Error received from element %s: %s" % (
                        message.src.get_name(), err))
                    print("Debugging information: %s" % debug)
                    break
                elif message.type == Gst.MessageType.WARNING:
                    warning, debug = message.parse_warning()
                    print(warning, debug)
                elif message.type == Gst.MessageType.EOS:
                    print("End-Of-Stream reached.")
                    break
                elif message.type == Gst.MessageType.STATE_CHANGED:
                    if isinstance(message.src, Gst.Pipeline):
                        old_state, new_state, pending_state = message.parse_state_changed()
                        print("Pipeline state changed from %s to %s." %
                              (old_state.value_nick, new_state.value_nick))
                else:
                    print("Unexpected message received.")

        # Stop pipline
        self.pipeline.set_state(Gst.State.NULL)


class RTSPSource(GstPipeline):
    def __init__(self, uri=None):
        self.location = '' if uri is None else 'location={}'.format(uri)
        self.pipeline = Gst.parse_launch(
            'rtspsrc name=source {} latency=0 udp-reconnect=1 timeout=5000000000 ! parsebin ! decodebin ! tee name=tee ! avenc_flv ! flvmux streamable=1 ! rtmpsink name=sink'.format(self.location)
        )


class UDPSource(GstPipeline):
    def __init__(self, uri=None):
        self.location = '' if uri is None else 'uri={}'.format(uri)
        self.pipeline = Gst.parse_launch(
            'udpsrc name=source uri={} ! application/x-rtp, media=(string)video, clock-rate=(int)90000, encoding-name=(string)H264 ! parsebin ! decodebin ! tee name=tee ! avenc_flv ! flvmux streamable=1 ! rtmpsink name=sink'.format(self.location)
        )


class UDP265Source(GstPipeline):
    def __init__(self, uri=None):
        self.location = '' if uri is None else 'uri={}'.format(uri)
        self.pipeline = Gst.parse_launch(
            'udpsrc name=source {} ! application/x-rtp, media=(string)video, clock-rate=(int)90000, encoding-name=(string)H265 ! parsebin ! decodebin ! tee name=tee ! avenc_flv ! flvmux streamable=1 ! rtmpsink name=sink'.format(self.location)
        )


class TestSource(GstPipeline):
    def __init__(self, uri=None):
        self.pipeline = Gst.parse_launch(
            'videotestsrc name=source ! tee name=tee ! x264enc ! flvmux streamable=1 ! rtmpsink name=sink'
        )

class Application(object):
    def __init__(self, optargs):
        (opts, args) = optargs

        if opts.stream_url is None or not opts.stream_url.startswith("rtmp://"):
            print("Unsupported stream URL %s" % opts.stream_url)
            sys.exit(1)

        self.pipeline = GstPipeline.from_uri(opts.input)
        if self.pipeline is None:
            print('Unsupported input: %s' % opts.input)
            sys.exit(1)

        rtmp = self.pipeline.sink()
        rtmp.set_property('location', opts.stream_url)

        self.exit = multiprocessing.Event()

        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self.pipeline.main_loop(self.exit)

    def exit_gracefully(self, signum, frame):
        self.exit.set()


def main():
    parser = OptionParser('wahaha [options]')
    parser.add_option("--input", dest="input", type="str",
                      help="Live Stream URL", default="videotestsrc")
    parser.add_option("--stream-url", dest="stream_url", type="str",
                      help="Stream URL", default=None)
    optsargs = parser.parse_args()
    (opts, args) = optsargs
    Application(optsargs)


if __name__ == '__main__':
    main()