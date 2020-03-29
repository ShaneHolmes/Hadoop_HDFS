package com.shane.mapreduce;

import java.util.Arrays;

public class Test {
    public int[] intersection(int[] nums1, int[] nums2) {
        Arrays.sort(nums1);
        Arrays.sort(nums2);
        int i=0,j=0,k=0;
        while(i<nums1.length&&j<nums2.length){
            if(nums1[i]<nums2[j]){
                i++;
            }else if(nums1[i]>nums2[j]){
                ++j;
            }else{
                nums1[k++]=nums2[j++];
                i++;
            }
        }
        int[] ints = Arrays.copyOfRange(nums1, 0, k);
        return ints;
    }
    public int arrangeCoins(int n) {
        int k=1;
        while(n-k>=0){
            n=n-k;
            k++;
        }
        return k-1;
    }

    public static void main(String[] args) {
        Test test=new Test();

//        int[] arr1=new int[]{1,2,2,1};
//        int[] arr2=new int[]{2,2};
//        int[] res=test.intersection(arr1,arr2);
//        for(int a:res){
//            System.out.println(a);
//        }

        System.out.println(test.arrangeCoins(3));
    }
}
