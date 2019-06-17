package demo;


import com.tencent.hippo.Message;
import com.tencent.hippo.client.MessageResult;
import com.tencent.hippo.client.consumer.ConsumerConfig;
import com.tencent.hippo.client.consumer.PullMessageConsumer;
import com.tencent.hippo.common.HippoConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *  Hippo is message broker component, This is used for testing consumer
 */
public class HippoConsumerDemo {
    /**logger**/
    private static Logger logger = LoggerFactory.getLogger(HippoConsumerDemo.class);

    //Hipper Controller address
    private static final String CONTROLLER_IP_LIST="**.**.**.**:8066";
    //Hipper Borker group
    private static final String GROUP_NAME="tx01_group";
    //Hipper Consumer Configure
    private static ConsumerConfig consumerConfig;
    /***Producer****/
    //Hipper topice accessKey(Note: Each topic's key and id are different)
    private static final String ACCESS_KEY="******";
    //Hipper topice accessId
    private static final String ACCESS_ID="*******";
    //Hipper access user
    private static final String USERNAME="admin";
    //Topic
    private static final String TOPIC="tx01";
    //Batch size
    private static final int BATCH_SIZE=10;
    //timeout
    private static final int TIME_OUT=10000;

    //Initial consumer configuration
    static {
        consumerConfig = new ConsumerConfig(CONTROLLER_IP_LIST,GROUP_NAME);
        consumerConfig.setSecretId(ACCESS_ID);
        consumerConfig.setSecretKey(ACCESS_KEY);
        consumerConfig.setUserName(USERNAME);
        consumerConfig.setConfirmTimeout(10000, TimeUnit.SECONDS);
    }

    /**
     * main for testing
     * @param args
     */
    public static void main(String[] args) throws Exception{
        //获取相关信息的comsumer
        PullMessageConsumer consumer = new PullMessageConsumer(consumerConfig);
        //每次接受信息的大小
        MessageResult result = consumer.receiveMessages(TOPIC,BATCH_SIZE,TIME_OUT);

        if(result.isSuccess()){
            List<Message> msgs = (List<Message>) result.getValue();
            for(Message msg:msgs){
                //header Information
                System.out.println(msg.getHeaders());
               // String msgBody = new String(msg.getData(), Charsets.UTF_8);
                String msgBody = new String(msg.getData());
            }

            boolean confirmed = result.getContext().confirm();

           if(!confirmed){
               System.out.println("result.getcontent.confirm");
           }else if(result.getCode()== HippoConstants.NO_MORE_MESSAGE){
               System.out.println("No more message for consumer ~");
               //if no more message, the thread could sheep 10 seconds
               Thread.sleep(10000);
           }else if(result.getCode() == HippoConstants.NOT_READY){
                System.out.println("Hippo is not ready");
           }else {
               System.out.println("Hippo "+result.getCode());
           }

           //消费者关闭
           consumer.shutdown();

        }
    }
}
