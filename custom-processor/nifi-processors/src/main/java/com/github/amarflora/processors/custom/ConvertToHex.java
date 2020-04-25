package com.github.amarflora.processors.custom;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.hortonworks.nifi.processors.util.FlowFileUtils;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"itc", "deserialize", "decode", "encode", "binary"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("This processor will convert the content of the flowfile or attribute to HEX")

@SeeAlso({EmpDecoder.class, ClassDDecoder.class})
public class ConvertToHex extends AbstractProcessor {

    static final PropertyDescriptor CONVERT_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Convert NiFi attributes to HEX")
            .displayName("Convert comma separated attributes to hex")
            .description("If set to true, specify the comma separated list of attributes to be converted. If false, it will convert the content.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
 
    static final PropertyDescriptor LIST_OF_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("List of attributes to convert")
            .displayName("List of attributes to convert")
            .description("The comma separated list of attributes to convert. The converted attribute will be prefixed with convert.tohex.output")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(Validator.VALID)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully converted message.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to convert message. Kindly look at the logs.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));
    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(CONVERT_ATTRIBUTES,LIST_OF_ATTRIBUTES));

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        FlowFile original = processSession.get();
        if (original == null) {
            return;
        }

        final byte[] data = FlowFileUtils.extractMessage(original, processSession);

		try {

			final String convertAttribute = processContext.getProperty(CONVERT_ATTRIBUTES).getValue();
			final String attributeList = processContext.getProperty(LIST_OF_ATTRIBUTES).getValue();

			if (convertAttribute.toLowerCase().equals("true")) {
				try {
					if (attributeList.isEmpty() || attributeList == null) {
						getLogger().warn("List of Attributes should be specified if "+CONVERT_ATTRIBUTES.getName()+" is set to True");
					} else {
						final String convertToHex = processContext.getProperty(LIST_OF_ATTRIBUTES)
								.evaluateAttributeExpressions(original).getValue();
						List<String> items = Arrays.asList(convertToHex.split("\\s*,\\s*"));

						for (int i = 0; i < items.size(); i++) {
							original = processSession.putAttribute(original, "convert.tohex.output." + i,
									new String(Hex.encodeHexString(items.get(i).getBytes())).toUpperCase());
						}
						processSession.transfer(original, REL_SUCCESS);
					}

				} catch (NullPointerException e) {
					getLogger().warn("List of Attributes should be specified if "+CONVERT_ATTRIBUTES.getName()+" is set to True");
					processSession.transfer(original, REL_FAILURE);

				}
			} else {
				original = processSession.write(original, (o) -> {
					o.write(new String(Hex.encodeHex(data)).getBytes());
				});
				processSession.transfer(original, REL_SUCCESS);
			}
		} catch (ProcessException e) {
			getLogger().warn("Exception Occured. {}", new Object[] { e });
			processSession.transfer(original, REL_FAILURE);
		}
    }
}
