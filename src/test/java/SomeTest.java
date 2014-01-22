import com.appmetr.s2s.AppMetr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

public class SomeTest {

    public static void main(String[] args) throws IOException {
        AppMetr appMetr = new AppMetr("someToken", "http://localhost/api");
        for (int i = 0; i < 1000; i++){
            HashMap<String, String> properties = new HashMap<String, String>();
            Random random = new Random();
            for(int j =0; j<25;j++){
                properties.put(String.valueOf(j), String.valueOf(random.nextLong()));
            }
            appMetr.track("event#"+i, properties);
        }
    }

}
