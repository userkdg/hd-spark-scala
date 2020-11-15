package com.kongdg.hive;

/**
 * @author userkdg
 * @date 2020-06-07 22:16
 **/
public class HiveClientTest {
    public static void main(String[] args) {

    }

    public static void sort(int[] arr, int start, int end){
        if (arr == null || arr.length==0)
            return;
        int p = p(arr, start, end);
        sort(arr, start, p);
        sort(arr, p+1, end);
    }

    public static int p(int[] arr, int start, int end){
        int tmp = arr[start];
        while (end > start){
            while (tmp <= arr[end] && start<end){
                --end;
            }
            if (start < end){
                arr[start] = arr[end];
                ++start;
            }
            while (tmp>=arr[start] && start<end){
                ++start;
            }
            if (start<end){
                arr[end] = arr[start];
                --end;
            }
        }
        arr[start] = tmp;
        return start;
    }

}
