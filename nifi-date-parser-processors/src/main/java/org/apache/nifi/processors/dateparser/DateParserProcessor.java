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

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class DateParserProcessor extends AbstractProcessor {

    public static final PropertyDescriptor READER = new PropertyDescriptor
            .Builder().name("date-parser-reader")
            .displayName("Record Reader")
            .description("The record reader for reading incoming data sets.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor WRITER = new PropertyDescriptor
            .Builder().name("date-parser-writer")
            .displayName("Record Writer")
            .description("The record writer for writing data sets.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successful flowfiles go to this relationship.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed flowfiles go to this relationship.")
            .build();

    public static final Relationship ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("On success, the original flowfile goes here.")
            .autoTerminateDefault(true)
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        READER, WRITER
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        SUCCESS, FAILURE, ORIGINAL
    )));

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    private RecordReaderFactory recordReaderFactory;
    private RecordSetWriterFactory recordSetWriterFactory;
    private RecordPathCache recordPathCache;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.recordReaderFactory = context.getProperty(READER).asControllerService(RecordReaderFactory.class);
        this.recordSetWriterFactory = context.getProperty(WRITER).asControllerService(RecordSetWriterFactory.class);
        this.recordPathCache = new RecordPathCache(50);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String name) {
        return new PropertyDescriptor.Builder()
            .name(name)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        List<ValidationResult> results = new ArrayList<>();

        long count = context.getProperties().keySet().stream().filter(prop -> prop.isDynamic()).count();

        if (count == 0) {
            results.add(new ValidationResult.Builder().valid(false).explanation("One or more properties mapping an input record " +
                    "path to an output record path must be provided.").build());
        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        FlowFile output = session.create(flowFile);
        try (InputStream is = session.read(flowFile);
             OutputStream os = session.write(output)) {
            List<Tuple<RecordPath, RecordPath>> paths = new ArrayList<>();
            context.getProperties().keySet().stream().filter(prop -> prop.isDynamic()).forEach(prop -> {
                RecordPath key = recordPathCache.getCompiled(prop.getName());
                String evaluated = context.getProperty(prop).evaluateAttributeExpressions(flowFile).getValue();
                RecordPath value = recordPathCache.getCompiled(evaluated);
                Tuple<RecordPath, RecordPath> temp = new Tuple<>(key, value);
                paths.add(temp);
            });

            RecordReader reader = recordReaderFactory.createRecordReader(flowFile, is, getLogger());
            RecordSetWriter writer = recordSetWriterFactory.createWriter(getLogger(), reader.getSchema(), os, flowFile);

            Record record = reader.nextRecord();
            writer.beginRecordSet();
            long count = 0;
            while (record != null) {
                processRecord(record, paths);
                writer.write(record);
                count++;
                record = reader.nextRecord();
            }
            writer.finishRecordSet();

            reader.close();
            writer.flush();
            os.close();
            is.close();

            output = session.putAttribute(output, "record.count", String.valueOf(count));

            session.transfer(output, SUCCESS);
            session.transfer(output, FAILURE);
        } catch (Exception ex) {
            getLogger().error("", ex);
            session.remove(output);
            session.transfer(flowFile, FAILURE);
        }
    }

    private void processRecord(Record record, List<Tuple<RecordPath, RecordPath>> recordPaths) {

    }
}
