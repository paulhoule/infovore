package com.ontology2.haruhi.fetchLogs;

import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.ontology2.centipede.shell.CommandLineApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.File;

import static com.google.common.base.Splitter.on;
import static com.google.common.collect.Iterables.getLast;

@Component
public class FetchLogs extends CommandLineApplication {
    static Logger logger = LoggerFactory.getLogger(FetchLogs.class);

    @Autowired
    TransferManager transferManager;
    @Resource
    String localLogTarget;
    @Resource
    String awsLogUri;

    @Override
    protected void _run(String[] strings) throws Exception {
        for (String that : strings) {
            doItForJob(that);
        }
    }

    private void doItForJob(String jobId) throws InterruptedException {
        logger.info("Beginning download of "+jobId);
        String bucketName= getLast(on("/").omitEmptyStrings().split(awsLogUri));
        Transfer that=transferManager.downloadDirectory(bucketName, jobId, new File(localLogTarget));
        that.waitForCompletion();
    }
}
