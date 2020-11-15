package com.kongdg.flume;

import com.clearspring.analytics.util.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.source.AbstractSource;

import java.io.Serializable;
import java.util.List;

/**
 * @author userkdg
 * @date 2020-05-28 14:55
 **/
public class FlumeDemo {
    public static class FlumeKafka2HdfsSource extends AbstractSource implements Serializable, Configurable, PollableSource {
        private static final long serialVersionUID = 1L;


        @Override
        public synchronized void start() {
            super.start();

        }

        @Override
        public synchronized void stop() {
            super.stop();
        }

        @Override
        public synchronized void setChannelProcessor(ChannelProcessor cp) {
            super.setChannelProcessor(cp);
        }

        @Override
        public synchronized void setName(String name) {
            super.setName(name);
        }

        @Override
        public Status process() throws EventDeliveryException {
            List<Event> events = Lists.newArrayList();
            JSONEvent event = new JSONEvent();
//            event.setBody();
//            event.setCharset();
//            event.setHeaders();
//            getChannelProcessor().processEventBatch();
            return null;
        }

        @Override
        public long getBackOffSleepIncrement() {
            return 0;
        }

        @Override
        public long getMaxBackOffSleepInterval() {
            return 0;
        }

        @Override
        public void configure(Context context) {

        }
    }
}
