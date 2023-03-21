package com.github.amarflora.processors.custom;

import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import java.time.DateTimeException;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"emp","decode", "flowfile", "encode"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Parses an incoming EMP message. The message is expected to be binary data or hex encoded binary data stored in the content of the flow file. Once parsed " +
        "the EMP header information is written as corresponding attributes. The payload of the EMP message is written directly as binary or hex encoded binary to the content of the flow file.")
@WritesAttributes({
        @WritesAttribute(attribute = "protocol.version", description = "Version of EMP header. Each version of the EMP message format specification will define the header version " +
                "number applicable to that version of the specification."),
        @WritesAttribute(attribute = "message.type", description = "Application message ID. The messaging specifications will assign unique Message Type (ID)s."),
        @WritesAttribute(attribute = "message.version", description = "Application message version. The messaging specifications will define the valid versions of each message and its content."),
        @WritesAttribute(attribute = "flags", description = "Flags indicating what options were used in constructing the header See paragraph 3.2.1.4 of EMP spec, “Flags,” for details."),
        @WritesAttribute(attribute = "data.length", description = "Size of message body"),
        @WritesAttribute(attribute = "message.number", description = "Application level message sequence number (bytes)"),
        @WritesAttribute(attribute = "message.time", description = "Time of message creation. 0 if not supported, see paragraph 3.2.2.2 of EMP spec, “Message Time Stamp,” for options and details."),
        @WritesAttribute(attribute = "event.time.utc", description = "The string representation of 'message.time.'"),
        @WritesAttribute(attribute = "variable.header.size", description = "Size of the variable portion of the header"),
        @WritesAttribute(attribute = "time.to.live", description = "Message time to live (seconds) Used to aid in routing and bridging."),
        @WritesAttribute(attribute = "routing.qos", description = "Message Quality of service. Used by wireless infrastructure to aid in network selection and prioritization."),
        @WritesAttribute(attribute = "source", description = "Message source address Null terminated string."),
        @WritesAttribute(attribute = "destination", description = "Message destination address Null terminated string."),
        @WritesAttribute(attribute = "data.integrity", description = "CRC or application-specific FCS (e.g., HMAC) 0 if not supported"),
})
@SeeAlso({ClassDDecoder.class})
public class EmpDecoder extends ProtocolDecoder<EmpMessage> {

    @OnScheduled
    public void onScheduled(final ProcessContext processContext) throws Exception {
        final String outgoingMessageFormat = processContext.getProperty(OUTGOING_DATA_FORMAT).getValue();
        final String convertToHEXOnly = processContext.getProperty(CONVERT_TO_HEX).getValue();
		try {
			if (TRUE.getValue().equals(convertToHEXOnly)) {
				if (BINARY.getValue().equals(outgoingMessageFormat)) {
					throw new Exception("Output format must be set to HEX if "+ CONVERT_TO_HEX.getDisplayName() +" is set to True {}");
				}
			}
		} catch (Exception e) {
			getLogger().error("Output format must be set to HEX if "+ CONVERT_TO_HEX.getDisplayName() +" is set to True {}",
					new Object[] { e });
			throw new Exception("Output format must be set to HEX if "+ CONVERT_TO_HEX.getDisplayName() +" is set to True {}");
		}
	}

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException, IllegalArgumentException {
        FlowFile original = processSession.get();
        if (original == null) {
            return;
        }

        final byte[] data = FlowFileUtils.extractMessage(original, processSession);
        final String incomingMessageFormat = processContext.getProperty(INCOMING_DATA_FORMAT).getValue();
        final String outgoingMessageFormat = processContext.getProperty(OUTGOING_DATA_FORMAT).getValue();
        final String convertToHEXOnly = processContext.getProperty(CONVERT_TO_HEX).getValue();

        try {
        	if (TRUE.getValue().equals(convertToHEXOnly))
        	{
        		original = processSession.write(original, (o) -> {
               if (HEX.getValue().equals(outgoingMessageFormat)) {
                    o.write(new String(Hex.encodeHex(data)).getBytes());
                }
            });
        		 processSession.transfer(original, REL_SUCCESS);
        	}
        	else
        	{
            final EmpMessage empMessageDecoded = decoded(incomingMessageFormat, data);
            original = processSession.putAttribute(original, "protocol.version", Integer.toString(empMessageDecoded.getProtocolVersion()));
            original = processSession.putAttribute(original, "message.type", Integer.toString(empMessageDecoded.getMessageType()));
            original = processSession.putAttribute(original, "message.version", Integer.toString(empMessageDecoded.getMessageVersion()));
            original = processSession.putAttribute(original, "flags", Integer.toString(empMessageDecoded.getFlags()));
            original = processSession.putAttribute(original, "data.length", Integer.toString(empMessageDecoded.getDataLength()));
            original = processSession.putAttribute(original, "message.number", Long.toString(empMessageDecoded.getMessageNumber()));
            original = processSession.putAttribute(original, "message.time", Long.toString(empMessageDecoded.getMessageTime()));
            original = processSession.putAttribute(original, "event.time.utc", getMessageTimeValue(empMessageDecoded));
            original = processSession.putAttribute(original, "variable.header.size", Integer.toString(empMessageDecoded.getVariableHeaderSize()));
            original = processSession.putAttribute(original, "time.to.live", Integer.toString(empMessageDecoded.getTimeToLive()));
            original = processSession.putAttribute(original, "routing.qos", Integer.toString(empMessageDecoded.getRoutingQoS()));
            original = processSession.putAttribute(original, "source", empMessageDecoded.getSource());
            original = processSession.putAttribute(original, "destination", empMessageDecoded.getDestination());
            original = processSession.putAttribute(original, "data.integrity", Long.toString(empMessageDecoded.getDataIntegrity()));
            original = processSession.write(original, (o) -> {
                if (BINARY.getValue().equals(outgoingMessageFormat)) {
                    o.write(empMessageDecoded.getMessageBody());
                } else if (HEX.getValue().equals(outgoingMessageFormat)) {
                    o.write(new String(Hex.encodeHex(empMessageDecoded.getMessageBody())).getBytes());
                }
            });
            processSession.transfer(original, REL_SUCCESS);
        }
       }
        catch (DeserializeException e) {
            getLogger().warn("Unable to decode the EMP message. {}", new Object[]{e});
            processSession.transfer(original, REL_FAILURE);
        }
        catch (IllegalArgumentException e) {
            getLogger().warn("Unable to decode the EMP message due to faulty incoming message. {}", new Object[]{e});
            processSession.transfer(original, REL_FAILURE);
        }
        
    }

    private String getMessageTimeValue(EmpMessage empMessageDecoded) {
        try {
            return empMessageDecoded.getEventTimeUtc();
        } catch (DateTimeException e) {
            throw new DeserializeException(e);
        }
    }

    @Override
    protected Deserializable<EmpMessage> createDeserializer() {
        return new EmpDeserializer(new DeserializeUtils(new EcefUtils()));
    }
}
