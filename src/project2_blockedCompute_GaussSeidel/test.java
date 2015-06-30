package project2_blockedCompute_GaussSeidel;

public class test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		for(long i=0; i<685230; i++)
			System.out.println(getBlockID_ofNode(i));
	}
	
	static long getBlockID_ofNode(Long nodeID)
	{	
		return   nodeID.hashCode()       %       GlobalVals.NUM_OF_BLOCKS;		
	}

}
