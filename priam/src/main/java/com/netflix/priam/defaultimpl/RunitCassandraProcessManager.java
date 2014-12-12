package com.netflix.priam.defaultimpl;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.Sleeper;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class RunitCassandraProcessManager extends CassandraProcessManager {
    private static final Logger logger = LoggerFactory.getLogger(RunitCassandraProcessManager.class);
    private static final int RUNIT_WAIT_SECONDS = 60;
    private static final int SCRIPT_EXECUTE_WAIT_TIME_MS = (RUNIT_WAIT_SECONDS + 10) * 1000;

    @Inject
    public RunitCassandraProcessManager(IConfiguration config, Sleeper sleeper)
    {
        super(config, sleeper);
    }

    @Override
    public void start(boolean ignored) throws IOException
    {
        logger.info("Starting cassandra server");
        ProcessBuilder startCass = controlProcess("up");

        Map<String, String> env = Maps.newHashMap();
        setEnv(env);

        for (Map.Entry<String, String> var : env.entrySet())
            FileUtils.writeStringToFile(new File("/etc/sv-env/cassandra/" + var.getKey()), var.getValue(), "UTF-8");

        startCass.directory(new File("/"));
        startCass.redirectErrorStream(true);
        logger.info("Start cmd: " + startCass.command().toString());
        Process starter = startCass.start();

        logger.info("Starting cassandra server ....");
        try {
            sleeper.sleepQuietly(SCRIPT_EXECUTE_WAIT_TIME_MS);
            int code = starter.exitValue();
            if (code == 0)
                logger.info("Cassandra server has been started");
            else
                logger.error("Unable to start cassandra server. Error code: {}", code);

            logProcessOutput(starter);
        } catch (Exception e)
        {
            logger.warn("Failed to start Cassandra", e);
        }

    }

    @Override
    public void stop() throws IOException
    {
        logger.info("Stopping cassandra server ....");
        ProcessBuilder stopCass = controlProcess("down");
        stopCass.directory(new File("/"));
        stopCass.redirectErrorStream(true);
        Process stopper = stopCass.start();

        sleeper.sleepQuietly(SCRIPT_EXECUTE_WAIT_TIME_MS);
        try
        {
            int code = stopper.exitValue();
            if (code == 0)
                logger.info("Cassandra server has been stopped");
            else
            {
                logger.error("Unable to stop cassandra server. Error code: {}", code);
                logProcessOutput(stopper);
            }
        }
        catch(Exception e)
        {
            logger.warn("Couldn't shut down cassandra properly", e);
        }
    }

    private ProcessBuilder controlProcess(String action)
    {
        return new ProcessBuilder(Arrays.asList("/sbin/sv", "-w", "" + RUNIT_WAIT_SECONDS, action, "cassandra"));
    }
}
