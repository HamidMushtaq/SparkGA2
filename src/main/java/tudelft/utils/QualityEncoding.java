/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package tudelft.utils;

import htsjdk.samtools.SAMRecord;

/**
 *
 * @author ddecap
 */
public class QualityEncoding {
    
    public enum QENCODING { 
        SANGER,
        ILLUMINA
    }
    
    private static final int REASONABLE_SANGER_THRESHOLD = 60;
    
    public static QENCODING guessEncoding(final SAMRecord read) throws QualityException {
        final byte[] quals = read.getBaseQualities();
        byte max = quals[0];
        for ( int i = 0; i < quals.length; i++ ) {
            if(quals[i] > max) max =quals[i];
        }
        System.out.println("Max quality: " + max);
        if(max <= REASONABLE_SANGER_THRESHOLD) {
            System.out.println("SANGER Quality encoding");
            return QENCODING.SANGER;
        } else {
            System.out.println("ILLUMINA Quality encoding");
            return QENCODING.ILLUMINA;
        }
    }
    
    private static final int fixQualityIlluminaToPhred = 31;  
    public static SAMRecord fixMisencodedQuals(final SAMRecord read) throws QualityException {
        final byte[] quals = read.getBaseQualities();
        for ( int i = 0; i < quals.length; i++ ) {
            quals[i] -= fixQualityIlluminaToPhred;
            if ( quals[i] < 0 )
                throw new QualityException(quals[i]);
        }
        read.setBaseQualities(quals);
        return read;
    }
}
