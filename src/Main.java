import java.io.IOException;

import org.apache.jena.riot.Lang;

import com.dadfha.mobisos.MobiSosCore;


public class Main {

	public static void main(String[] args) throws IOException {
		
		for(int i=0; i < MobiSosCore.TESTCASE_N; i++) {
			
			MobiSosCore.USER_N = MobiSosCore.USER_N_TESTCASE[i];
			MobiSosCore.CHECKIN_N = MobiSosCore.CHECKIN_N_TESTCASE[i];
			MobiSosCore.WIFI_ROUTER_N = MobiSosCore.WIFI_ROUTER_N_TESTCASE[i];
			MobiSosCore.TAG_N = MobiSosCore.TAG_N_TESTCASE[i];
			MobiSosCore.CAMERA_N = MobiSosCore.CAMERA_N_TESTCASE[i];
			
			MobiSosCore.CURRENT_TESTCASE = i;
			
			MobiSosCore core = new MobiSosCore(false, false);		
			core.beginTest();
			
			core = null;			
			System.gc();			
		}
		
		System.exit(0);

			
		//Resource user = core.createUser("nabito@gmail.com", "mypwd");
		//String uid = user.getProperty(MobiSosCore.PROP_UUID).getString();
		
		//System.out.println(uid);
		
		
		//String titaLoc = "{'accuracy': 100,'altitude': 0,'altitudeAccuracy': null,'heading': 0,'latitude': 40.493781233333333,'longitude': -80.056671,'speed': 0,'timestamp': 1318426498331}";
		// if MAC address is to be used as part of resource ID (local name) it must not have ':'
		//core.checkInWifi(uid, titaLoc, "aabbccddeeff");
		//core.checkInWifi(uid, titaLoc, "03a004d30011");
		
		//core.addSamples();
		
		//core.sosCall("a642d786-f44e-48fb-afe2-8e334466b5a3", "E347F181-A1E6-44EF-A2BA-C01A9A63291F", titaLoc);
		
		//http://192.168.10.150:1337/checkin/20e58795-2f67-45e4-8f22-d5f10cae046a/1390583880056/1390670280056
		//String s = core.getCheckinJson("4d2c6f56-418c-4dac-8c9d-b21245e17d88", 1390583880056L, System.currentTimeMillis());
		//System.out.println("aha" + s);

		//core.dumpTDB();
		
		
		//System.out.println("Main class is intended for testing purpose only. Statements will be written to TDB and dump.");


	}

}
