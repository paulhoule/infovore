//package com.ontology2.haruhi.accountant;
//
//import com.amazonaws.auth.AWS3Signer;
//import com.amazonaws.auth.AWSCredentials;
//import com.amazonaws.services.s3.AmazonS3Client;
//import com.amazonaws.services.s3.model.ObjectListing;
//import com.amazonaws.services.s3.model.S3Object;
//import com.amazonaws.services.s3.model.S3ObjectInputStream;
//import com.amazonaws.services.s3.model.S3ObjectSummary;
//import com.google.common.collect.ArrayListMultimap;
//import com.google.common.collect.ListMultimap;
//import com.google.common.collect.Maps;
//import com.google.common.collect.Multimap;
//import com.ontology2.centipede.shell.CommandLineApplication;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Component;
//import org.supercsv.io.CsvListReader;
//import org.supercsv.io.CsvMapReader;
//import org.supercsv.prefs.CsvPreference;
//
//import java.io.BufferedReader;
//import java.io.InputStreamReader;
//import java.math.BigDecimal;
//import java.text.DecimalFormat;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.List;
//import java.util.Map;
//import java.util.regex.Pattern;
//
//@Component("accountant")
//public class Accountant extends CommandLineApplication {
//
//    @Autowired
//    String awsBillingBucket;
//    @Autowired
//    AWSCredentials awsCredentials;
//
//    @Override
//    protected void _run(String[] strings) throws Exception {
//        // TODO: year-month should be selectable
//        // TODO: optional detailed report that shows all line items
//        // TODO: selectivity for a particular job
//        // TODO: cache cost-allocation report (it's less than 100 kb for me so this is no hurry)
//        // TODO: merge in data from EMR (job start time, job name) to make report more comprehensive
//
//        String yrmo = new SimpleDateFormat("yyyy-MM").format(new Date());
//
//        AmazonS3Client client = new AmazonS3Client(awsCredentials);
//        String reportKey = findCostAllocationReport(yrmo, client);
//        if (reportKey == null) {
//            throw new Exception("Could not find cost allocation report");
//        }
//        S3Object object = client.getObject(awsBillingBucket, reportKey);
//        S3ObjectInputStream inputStream = object.getObjectContent();
//        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
//        CsvListReader csv = new CsvListReader(reader, CsvPreference.STANDARD_PREFERENCE);
//        List<String> warning = csv.read();
//        List<String> headersList = csv.read();
//        Map<String, Integer> headers = Maps.newHashMap();
//        for (int i = 0; i < headersList.size(); i++)
//            headers.put(headersList.get(i), i);
//
//        int jobFlowColumn = headers.get("user:com.ontology2.jobFlowId");
//        int totalCostColumn = headers.get("TotalCost");
//
//        DecimalFormat fmt = new DecimalFormat();
//        fmt.setParseBigDecimal(true);
//        Multimap<String,LineItem> items= ArrayListMultimap.create();
//
//        while (true) {
//            List<String> dataRow = csv.read();
//            if (dataRow == null)
//                break;
//
//            String jobFlowId = dataRow.get(jobFlowColumn);
//            String totalCost = dataRow.get(totalCostColumn);
//
//            if (jobFlowId != null) {
//                items.put(jobFlowId,new LineItem(jobFlowId,(BigDecimal) fmt.parse(totalCost)));
//            }
//        }
//
//        for(String jobFlowId:items.keySet()) {
//            BigDecimal sum=BigDecimal.ZERO;
//            for(LineItem i:items.get(jobFlowId)) {
//                sum=sum.add(i.amount);
//            }
//
//            System.out.println(String.format("%16s   %7.2f",jobFlowId,sum));
//        }
//    }
//
//    private String findCostAllocationReport(String yrmo, AmazonS3Client client) {
//        ObjectListing listing = client.listObjects(awsBillingBucket);
//        List<S3ObjectSummary> items = listing.getObjectSummaries();
//        Pattern p = Pattern.compile("^[0-9]*-aws-cost-allocation-" + yrmo + ".csv");
//        for (S3ObjectSummary item : items) {
//            String key = item.getKey();
//            if (p.matcher(key).matches())
//                return key;
//        }
//
//        return null;
//    }
//
//    public class LineItem {
//        public final String jobFlowId;
//        public final BigDecimal amount;
//
//        public LineItem(String jobFlowId, BigDecimal amount) {
//            this.jobFlowId = jobFlowId;
//            this.amount = amount;
//        }
//    }
//}
