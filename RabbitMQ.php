<?php
namespace Haodingdan\BusinessModule\Channel;
use Haodingdan\BusinessModule\ModuleManager;

if(!class_exists("AMQPConnection")){
require_once __DIR__.'/amqp/amqp.inc';
}

class RabbitMQ {
	
	static $connection;
	static $channel_pool;
	
    function __construct() {
    }

    static function sendPhoneMessage($data = null){
    	if(!empty($data) && is_array($data)){
    		RabbitMQ::publishMessage("sms", $data, "sms.send", 'topic');
    	}
    }
    /**
     * 发送邮件消息到队列
     * @param null $data
     * @param bool $set_priority 是否设置优先权 此功能暂时只是设置
     * @return bool
     */
    static function sendMail($data = null, $set_priority = true){
        if(!empty($data) && is_array($data)){
            /*
            $message = array(
                'recipient' => trim($data['recipient']),	//收件人
                'subject'		=> $data['subject'],		//邮件标题
                'content'   => $data['content']		//邮件内容
            );

            //其他信息  比如bcc
            if( isset( $data['other'] ) ){
                $message['other'] = $data['other'];
            }
            */
            $data['recipient'] = trim($data['recipient']);
            if($set_priority) {
                $email_splits = explode("@", $data['recipient']);
                if(count($email_splits) !== 2){
                    return false;
                }
                $mail_domain = strtolower($email_splits[1]);
                $domain_config = array('qq.com'=>'qq',
                    '163.com'=>'163',
                    '126.com'=>'126');

                $domain_nick_name = "others";
                foreach ($domain_config as $domain=>$name){
                    if($mail_domain == $domain){
                        $domain_nick_name = $name;
                        break;
                    } else{
                        $subdomain = ".".$domain;
                        $strpos = strpos($mail_domain, $subdomain);
                        if($strpos >= 0 &&($strpos == strlen($mail_domain) - strlen($subdomain))){
                            $domain_nick_name = $name;
                            break;
                        }
                    }
                }
                $priority = isset($data['priority']) ? $data['priority'] : 0;
                if($priority > 0){
                    self::publishMessage("email_delay_{$domain_nick_name}", $data);
                }
                else{
                    self::publishMessage("email", $data);
                }
            }else{
                self::publishMessage("email", $data);
            }
        }
    }

    /**
     * @param string $type 交换机类型
     * @param array $data 数据
     * @param string $routing_key 路由key
     */
    static function publishMessage($type, $data, $routing_key="", $exchange_type = 'direct'){
//         $config = ModuleManager::$web_config['msgqueue_server'];
//         $host = $config['host'];
//         $port = $config['port'];
//         $user = $config['user'];
//         $pass = $config['pass'];
//         $vhost = $config['vhost'];

        $exchange = $type . '_message_exchange';
        $queue = $type . '_message_queue';

        $ch_id = self::getUserDefinedChannelId($exchange,$exchange_type);
        //$conn = new \AMQPConnection($host, $port, $user, $pass, $vhost);
        try{
        	self::initUserDefinedConnection($exchange,$exchange_type);
	        ////$ch = $conn->channel();
	        //$ch->queue_declare($queue, false, true, false, false);
	        //self::$public_channel->exchange_declare($exchange, $exchange_type, false, true, false);
	        //$ch->queue_bind($queue, $exchange);
	        $msg_body = json_encode($data);
	        $msg = new \AMQPMessage($msg_body,
	            array(
	                'content_type' => 'application/json',
	                'delivery_mode'=> 2
	            )
	        );
	        
	        self::$channel_pool[$ch_id]->basic_publish($msg, $exchange,$routing_key);
        }catch(Exception $e){
        	var_dump($e);
        	self::initUserDefinedConnection($exchange,$exchange_type);
        	////$ch = $conn->channel();
        	//$ch->queue_declare($queue, false, true, false, false);
        	//self::$public_channel->exchange_declare($exchange, $exchange_type, false, true, false);
        	//$ch->queue_bind($queue, $exchange);
        	$msg_body = json_encode($data);
        	$msg = new \AMQPMessage($msg_body,
        			array(
        					'content_type' => 'application/json',
        					'delivery_mode'=> 2
        			)
        	);
        	
        	self::$channel_pool[$ch_id]->basic_publish($msg, $exchange,$routing_key);
        }
        //$ch->close();
        //$conn->close();
    }

    /**
     * 发送私信消息到队列
     * @param string $route_key 路由key
     * @param array $data 数据数组
     */
    static function publishEnquiryMsg($route_key, $data){

        $exchange = 'enquiry_msg_exchange';
        $exchange_type = "topic";
        $ch_id = self::getUserDefinedChannelId($exchange,$exchange_type);
        self::initUserDefinedConnection($exchange,$exchange_type);
        //$conn = self::getSingleConnection();
        //$ch = $conn->channel();
        //self::$public_channel->exchange_declare($exchange, 'topic', false, true, false);
        //取得操作时间
        $data['time'] = time();
        $msg_body = json_encode($data);
        $msg = new \AMQPMessage($msg_body,
            array(
                'content_type' => 'application/json',
                'delivery_mode'=> 2
            )
        );
        self::$channel_pool[$ch_id]->basic_publish($msg, $exchange, $route_key);
        //$ch->close();
        //$conn->close();
    }
    
    
    static function publishTradeMsg($route_key, $data){
    	//$conn = self::getSingleConnection();
    	//$ch = $conn->channel();
    	$exchange = 'trade_msg_exchange';
    	$exchange_type = "topic";
    	$ch_id = self::getUserDefinedChannelId($exchange,$exchange_type);
    	self::initUserDefinedConnection($exchange,$exchange_type);
    	//self::$public_channel->exchange_declare($exchange, 'topic', false, true, false);
    	$data['time'] = time();
    	$msg_body = json_encode($data);
    	$msg = new \AMQPMessage($msg_body,
    			array(
    					'content_type' => 'application/json',
    					'delivery_mode'=> 2
    			)
    	);
    	self::$channel_pool[$ch_id]->basic_publish($msg, $exchange, $route_key);
    	//$ch->close();
    	//$conn->close();
    }
    
    
    static function publishSixinMsg($route_key, $data){
    	//$conn = self::getSingleConnection();
    	//$ch = $conn->channel();
    	$exchange = 'danxin_message_exchange';
    	$exchange_type = "topic";
    	$ch_id = self::getUserDefinedChannelId($exchange,$exchange_type);
    	self::initUserDefinedConnection($exchange,$exchange_type);
    	//self::$public_channel->exchange_declare($exchange, 'topic', false, true, false);
    	$msg_body = json_encode($data);
    	$msg = new \AMQPMessage($msg_body,
    			array(
    					'content_type' => 'application/json',
    					'delivery_mode'=> 2
    			)
    	);
    	self::$channel_pool[$ch_id]->basic_publish($msg, $exchange, $route_key);
    	//$ch->close();
    	//$conn->close();
    }
    static function publishSixinMsgDelay($route_key, $data){
    	//$conn = self::getSingleConnection();
    	//$ch = $conn->channel();
    	$exchange = 'danxin_message_delay_exchange';
    	$exchange_type = "topic";
    	$ch_id = self::getUserDefinedChannelId($exchange,$exchange_type);
    	self::initUserDefinedConnection($exchange,$exchange_type);
    	//self::$public_channel->exchange_declare($exchange, 'topic', false, true, false);
    	$msg_body = json_encode($data);
    	$msg = new \AMQPMessage($msg_body,
    			array(
    					'content_type' => 'application/json',
    					'delivery_mode'=> 2
    			)
    	);
    	self::$channel_pool[$ch_id]->basic_publish($msg, $exchange, $route_key);
    	//$ch->close();
    	//$conn->close();
    }
    
    
    static function publishHddMsg($route_key, $data){
    	//$conn = self::getSingleConnection();
    	//$ch = $conn->channel();
    	$exchange = 'hdd_message_exchange';
    	$exchange_type = "topic";
    	$ch_id = self::getUserDefinedChannelId($exchange,$exchange_type);
    	self::initUserDefinedConnection($exchange,$exchange_type);
    	//self::$public_channel->exchange_declare($exchange, 'topic', false, true, false);
    	$msg_body = json_encode($data);
    	$msg = new \AMQPMessage($msg_body,
    			array(
    					'content_type' => 'application/json',
    					'delivery_mode'=> 2
    			)
    	);
    	self::$channel_pool[$ch_id]->basic_publish($msg, $exchange, $route_key);
    	//$ch->close();
    	//$conn->close();
    }
    
    /**
     * @param $exchange
     * @param $exchange_type
     * */
    static function initUserDefinedConnection($exchange="",$exchange_type=""){
    	$ch_id = self::getUserDefinedChannelId($exchange,$exchange_type);
    	//没有链接先生成链接
    	if(empty(self::$connection) || !isset(self::$connection->sock)){
//     		echo "reconnect sock........\n";
	    	$config = ModuleManager::$web_config['msgqueue_server'];
	    	$host = $config['host'];
	    	$port = $config['port'];
	    	$user = $config['user'];
	    	$pass = $config['pass'];
	    	$vhost = $config['vhost'];
	    	self::$connection = new \AMQPConnection($host, $port, $user, $pass, $vhost);
	    	$channel = self::$connection->channel();
	    	$channel->exchange_declare($exchange, $exchange_type, false, true, false);
	    	self::$channel_pool[$ch_id] = $channel;
    	}else{ //没有channel  生成channel
    		if(!isset(self::$channel_pool[$ch_id])){
//     			echo "generate channel  with exchange:$exchange,exchange_type:$exchange_type \n";
    			$channel = self::$connection->channel();
//     			echo "get new channel \n";
    			$channel->exchange_declare($exchange, $exchange_type, false, true, false);
//     			echo "exchange declared in channel \n";
    			self::$channel_pool[$ch_id] = $channel;
//     			echo "set channel to channel pool  done....\n";
    		}
    	}
    	//return self::$connection;
    }
    
    static function getConnection(){
    		$config = ModuleManager::$web_config['msgqueue_server'];
    		$host = $config['host'];
    		$port = $config['port'];
    		$user = $config['user'];
    		$pass = $config['pass'];
    		$vhost = $config['vhost'];
    		return new \AMQPConnection($host, $port, $user, $pass, $vhost);
    }
    
    /**
     * @param param1 usually be exchange name   like mail_exchange
     * @param param2 usually be type            like topic direct
     * */
    private static function getUserDefinedChannelId($exchange_name='',$exchange_type=''){
    	if(empty($exchange_name) && empty($exchange_type)){
    		return NULL;
    	}else{
    		return md5($exchange_name."-".$exchange_type);
    	}
    }
}

?>