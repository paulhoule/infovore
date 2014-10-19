package com.ontology2.haruhi.alert;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component("alertService")
public class AlertService {
    @Autowired
    AmazonSQS sqsClient;
    @Resource
    String sqsQueueUrl;

    public void alert(String message) {
        sqsClient.sendMessage(new SendMessageRequest().withMessageBody(message).withQueueUrl(sqsQueueUrl));
    }
}
