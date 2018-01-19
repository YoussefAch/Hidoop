package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;

public class CallBackImpl extends UnicastRemoteObject implements CallBack{

	
	private Semaphore called;
	
	
	public CallBackImpl() throws RemoteException {
		this.called = new Semaphore(1);
		try {
			this.called.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void onFinished() {
		this.called.release();
	}


	@Override
	public Semaphore getCalled() {
		return this.called;
	}
	

}
