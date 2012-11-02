package com.ontology2.millipede.sink;

public abstract class GroupingSink<T> implements Sink<T> {

	private Object groupKey=null;
	
	public final void accept(T obj) throws Exception {		
		Object newKey=computeGroupKey(obj);
		if(newKey==null)
			throw new Exception("Group key cannot be null");
		
		if (!newKey.equals(groupKey)) {
			if (groupKey!=null) closeGroup();
			groupKey=newKey;
			openGroup();
		}
		
		acceptItem(obj);
	}
	
	protected Object getGroupKey() {
		return groupKey;
	}
	
	@Override
	
	public final void close() throws Exception {
		closeGroup();
		close$();
	}
	
	abstract protected void close$() throws Exception;

	protected abstract void closeGroup() throws Exception;
	protected abstract void openGroup() throws Exception;
	protected abstract void acceptItem(T obj) throws Exception;

	
	// must return something not null as the group key,  the only important
	// thing is that it must respect the equals operator
	
	protected abstract Object computeGroupKey(T obj);

}
