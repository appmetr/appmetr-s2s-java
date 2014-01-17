import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class SomeTest {
    public static void main(String[] args) throws IOException {
        FileWriter writer = new FileWriter("lalala");
        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        bufferedWriter.append('s');
        bufferedWriter.close();
        System.out.println("fileWriter == null ?" + bufferedWriter == null);
    }
}
