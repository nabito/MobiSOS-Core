import com.dadfha.mobisos.MobiSosCore;
import com.hp.hpl.jena.rdf.model.Resource;


public class Main {

	public static void main(String[] args) {
		
		MobiSosCore core = new MobiSosCore();
		
		Resource user = core.createUser("nabito@gmail.com", "mypwd");
		String uid = user.getProperty(core.PROP_UUID).getString();
		
		System.out.println(uid);
		
		String titaLoc = "{'accuracy': 100,'altitude': 0,'altitudeAccuracy': null,'heading': 0,'latitude': 40.493781233333333,'longitude': -80.056671,'speed': 0,'timestamp': 1318426498331}";
		// if MAC address is to be used as part of resource ID (local name) it must not have ':'
		core.checkInWifi(uid, titaLoc, "aabbccddeeff");
		core.checkInWifi(uid, titaLoc, "03a004d30011");
		//core.checkInAtWifi("3102201656413", titaLoc, "aa:bb:cc:dd:ee:ff");
		
		core.dumpTDB();
		
		System.out.println("Main class is intended for testing purpose only. Statements will be written to TDB and dump.");

	}

}
