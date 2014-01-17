package com.appmetr;

import java.util.Random;

public class BatchIdRandomGenerator {

    public static int getBatchId(){
        return new Random().nextInt();
    }
}
