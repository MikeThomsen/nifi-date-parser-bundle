/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.dateparser;

import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;


public class DateParserProcessorTest {

    private TestRunner testRunner;
    private MockRecordParser parser;

    @Before
    public void init() throws Exception {
        parser = new MockRecordParser();
        parser.addSchemaField(new RecordField("date", RecordFieldType.STRING.getDataType()));
        MockRecordWriter writer = new MockRecordWriter();
        testRunner = TestRunners.newTestRunner(DateParserProcessor.class);
        testRunner.addControllerService("reader", parser);
        testRunner.addControllerService("writer", writer);
        testRunner.setProperty(DateParserProcessor.WRITER, "writer");
        testRunner.setProperty(DateParserProcessor.READER, "reader");
        testRunner.setProperty("/date", "/date");
        testRunner.enableControllerService(parser);
        testRunner.enableControllerService(writer);
        testRunner.assertValid();
    }

    private void assertCounts(int success, int original, int failure) {
        testRunner.assertTransferCount(DateParserProcessor.ORIGINAL, original);
        testRunner.assertTransferCount(DateParserProcessor.SUCCESS, success);
        testRunner.assertTransferCount(DateParserProcessor.FAILURE, failure);
    }

    private void testPattern(String input, String expected) {
        parser.addRecord(input);
        testRunner.enqueue("");
        testRunner.run();

        assertCounts(1, 1, 0);

        List<MockFlowFile> flowFileList = testRunner.getFlowFilesForRelationship(DateParserProcessor.SUCCESS);

        byte[] raw = testRunner.getContentAsByteArray(flowFileList.get(0));
        String value = new String(raw);
        assertEquals(expected, value.trim().replaceAll("\"", ""));
    }

    @Test
    public void testPattern1() {
        testPattern("Tue, 3 Jul 2018\r\n 12:00:39 +0000", "2018-07-03T08:00:39 -0400");
    }

    @Test
    public void testPattern2() {
        testPattern("Tue, 3 Jul 2018 07:10:41 -0500", "2018-07-03T08:10:41 -0400");
    }

    @Test
    public void testPattern3() {
        testPattern("Mon, 02 Jul 2018 15:59:03 -0700 (PDT)", "2018-07-02T18:59:03 -0400");
    }

    @Test
    public void testPattern4() {
        testPattern("Fri, \r\n\t20 Nov 2009 21:19:41 -0800 (PST)", "2009-11-21T00:19:41 -0500");
    }

    @Test
    public void testPattern5() {
        testPattern("Wed, 18 Jul 2018 01:47:00 +0000 (UTC)", "2018-07-17T21:47:00 -0400");
    }

    @Test
    public void testPattern6() {
        testPattern("Tue, 26 Jun 2012 06:50:04 -0000", "2012-06-26T02:50:04 -0400");
    }

    @Test
    public void testPattern7() {
        testPattern("20 Jun 2010 12:19:15 -0000", "2010-06-20T08:19:15 -0400");
    }

    @Test
    public void testPattern8() {
        testPattern("Thu, 22 Apr 2004 09:27:40 -0400 (=?ISO-8859-1?Q?Est_=28heure_d'=E9t=E9=29?=)", "2004-04-22T09:27:40 -0400");
    }

    @Test
    public void testPattern9() {
        testPattern("Fri, 1 Jul 2005 14:33:47 -0400 (EDT)\r\n\t[ConcentricHost SMTP Relay 1.17]", "2005-07-01T14:33:47 -0400");
    }

    @Test
    public void testPattern10() {
        testPattern("2019-12-31T19:35:00-0500", "2019-12-31T19:35:00 -0500");
    }

    @Test
    public void testPattern11() {
        testPattern("2020/01/01 17:35:31", "2020-01-01T17:35:31 -0500");
    }

    @Test
    public void testPattern12() {
        testPattern("01/15/2020 17:35:31", "2020-01-15T17:35:31 -0500");
    }
}
