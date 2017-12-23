import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class SimpleDistributedLockMutex extends BaseDistributedLock implements
		DistributedLock {
	
	//锁名称前缀，用这个名称创建完临时顺序节点，zookeeper会返回lock-0000000001,lock-000000002这种类型的节点
	private static final String LOCK_NAME = "lock-";
			
	private final String basePath; //zookeeper父级锁节点路径 /locker
	
	private String ourLockPath; //记录获取到的锁节点路径

    /**
     *尝试获取锁
     * @param time
     * @param unit
     * @return
     * @throws Exception
     */
    private boolean internalLock(long time, TimeUnit unit) throws Exception
    {

    	ourLockPath = attemptLock(time, unit);
        return ourLockPath != null;
        
    }
    
    public SimpleDistributedLockMutex(ZkClientExt client, String basePath){
    	    	
    	super(client,basePath,LOCK_NAME);
    	this.basePath = basePath;
    	
    }


    /**
     * 获取锁
     * @throws Exception
     */
	public void acquire() throws Exception {
		
        if ( !internalLock(-1, null) )  //永不超时
        {
            throw new IOException("连接丢失!在路径:'"+basePath+"'下不能获取锁!");
        }
	}

    /**
     * 获取锁直到超时
     * @param time
     * @param unit
     * @return
     * @throws Exception
     */
	public boolean acquire(long time, TimeUnit unit) throws Exception {

		return internalLock(time, unit);
	}

    /**
     * 释放锁
     * @throws Exception
     */
	public void release() throws Exception {
	    
		releaseLock(ourLockPath);
	}


}
