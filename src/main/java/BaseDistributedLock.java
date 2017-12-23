import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 基础分布式锁类，主要用于和zookeeper交互
 */
public class BaseDistributedLock {
	
    private final ZkClientExt client; //zookeeper客户端
    private final String  path;
    private final String  basePath;
    private final String  lockName;
    private static final Integer  MAX_RETRY_COUNT = 10; //最大重试次数
    	
	public BaseDistributedLock(ZkClientExt client, String path, String lockName){

        this.client = client;
        this.basePath = path;
        this.path = path.concat("/").concat(lockName);		
		this.lockName = lockName;
		
	}

    /**
     * 删除zookeeper中节点
     * @param ourPath
     * @throws Exception
     */
	private void deleteOurPath(String ourPath) throws Exception{
		client.delete(ourPath);
	}

    /**
     * zookeeper中创建临时顺序节点
     * @param client
     * @param path
     * @return
     * @throws Exception
     */
	private String createLockNode(ZkClient client,  String path) throws Exception{
		if(!client.exists("/Mutex")){
            client.createPersistent("/Mutex");
        }
		return client.createEphemeralSequential(path, null);
	}
	
	private boolean waitToLock(long startMillis, Long millisToWait, String ourPath) throws Exception{
        
		boolean  haveTheLock = false;
        boolean  doDelete = false;
        
        try
        {
 
            while ( !haveTheLock )
            {
                List<String> children = getSortedChildren();
                String sequenceNodeName = ourPath.substring(basePath.length()+1); //获取当前节点的序列化值

                int  ourIndex = children.indexOf(sequenceNodeName);
                if ( ourIndex<0 ){ //由于网络闪断，可能会将刚创建的节点删除，这时抛出异常交由上级处理
                	throw new ZkNoNodeException("节点没有找到: " + sequenceNodeName);
                }

                boolean isGetTheLock = ourIndex == 0;
                String  pathToWatch = isGetTheLock ? null : children.get(ourIndex - 1); //如果当前节点是最小节点，那么获取到锁，否则监听比自己小的节点的删除事件

                if ( isGetTheLock ){ //获得锁
                	
                    haveTheLock = true;
                    
                }else{
                	
                    String  previousSequencePath = basePath .concat( "/" ) .concat( pathToWatch );
                    //CountDownLatch这个类能够使一个线程等待其他线程完成各自的工作后再执行。
                    // 例如，应用程序的主线程希望在负责启动框架服务的线程已经启动所有的框架服务之后
                    // 再执行。
                    //CountDownLatch是通过一个计数器来实现的，
                    // 计数器的初始值为线程的数量。
                    // 每当一个线程完成了自己的任务后，计数器的值就会减1。
                    // 当计数器值到达0时，它表示所有的线程已经完成了任务，
                    // 然后在闭锁上等待的线程就可以恢复执行任务。
                    final CountDownLatch    latch = new CountDownLatch(1);
                    //监听比自己小的节点的删除事件，
                    final IZkDataListener previousListener = new IZkDataListener() {
                		
                		public void handleDataDeleted(String dataPath) throws Exception {
                			latch.countDown();	//结束等待
                		}
                		
                		public void handleDataChange(String dataPath, Object data) throws Exception {
                			// ignore									
                		}
                	};

                    try 
                    {                  
						//如果节点不存在会出现异常
                        //订阅节点改变事件
                    	client.subscribeDataChanges(previousSequencePath, previousListener);
                    	
                        if ( millisToWait != null )
                        {
                            millisToWait -= (System.currentTimeMillis() - startMillis);
                            startMillis = System.currentTimeMillis();
                            if ( millisToWait <= 0 )
                            {
                                doDelete = true;    // timed out - delete our node
                                break;
                            }

                            latch.await(millisToWait, TimeUnit.MICROSECONDS); //等待直到超时
                        }
                        else
                        {
                        	latch.await(); //等待
                        }
                    }
                    catch ( ZkNoNodeException e ) 
                    {
                        //ignore
                    }finally{
                    	client.unsubscribeDataChanges(previousSequencePath, previousListener);
                    }

                }
            }
        }
        catch ( Exception e )
        {
            //发生异常需要删除节点
            doDelete = true;
            throw e;
        }
        finally
        {
            //如果需要删除节点
            if ( doDelete )
            {
                deleteOurPath(ourPath);
            }
        }
        return haveTheLock;
	}


    /**
     * 获取临时节点中的序号值
     * @param str
     * @param lockName
     * @return
     */
    private String getLockNodeNumber(String str, String lockName)
    {
        int index = str.lastIndexOf(lockName);
        if ( index >= 0 )
        {
            index += lockName.length();
            return index <= str.length() ? str.substring(index) : "";
        }
        return str;
    }

    /**
     * 获取zookeeper中排好序的子节点列表
     * @return
     * @throws Exception
     */
    List<String> getSortedChildren() throws Exception
    {
    	try{
    		
	        List<String> children = client.getChildren(basePath);
	        Collections.sort
	        (
	        	children,
	            new Comparator<String>()
	            {
	                public int compare(String lhs, String rhs)
	                {
	                    return getLockNodeNumber(lhs, lockName).compareTo(getLockNodeNumber(rhs, lockName));
	                }
	            }
	        );
	        return children;
	        
    	}catch(ZkNoNodeException e){
    		
    		client.createPersistent(basePath, true);
    		return getSortedChildren();
    		
    	}
    }


    /**
     * 释放锁,就是删除zookeeper临时节点
     * @param lockPath
     * @throws Exception
     */
	protected void releaseLock(String lockPath) throws Exception{
		deleteOurPath(lockPath);	
		
	}

    /**
     * 尝试获取锁
     * @param time  //等待时间
     * @param unit  //时间单位
     * @return
     * @throws Exception
     */
	protected String attemptLock(long time, TimeUnit unit) throws Exception{
		
        final long      startMillis = System.currentTimeMillis();
        final Long      millisToWait = (unit != null) ? unit.toMillis(time) : null;

        String          ourPath = null;
        boolean         hasTheLock = false;
        boolean         isDone = false;
        int             retryCount = 0;
        
        //网络闪断需要重试，知道大于最大重试次数
        while ( !isDone )
        {
            isDone = true;

            try
            {
                ourPath = createLockNode(client, path); //在zookeeper中创建锁节点
                hasTheLock = waitToLock(startMillis, millisToWait, ourPath); //获取锁
            }
            catch ( ZkNoNodeException e ) //网络闪断抛出异常，需要重试
            {
                if ( retryCount++ < MAX_RETRY_COUNT )
                {
                    isDone = false;
                }
                else
                {
                    throw e;
                }
            }
        }
        if ( hasTheLock )
        {
            return ourPath;
        }

        return null;
	}
	
	
}