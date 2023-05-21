/*******************************************************************************
 * -----------------------------------------------------------------------------
 * <br>
 * <p><b>Copyright (c) 2006 InteQ Corporation. All Rights Reserved.</b>
 * <br>
 * <br>
 * This SOURCE CODE FILE, which has been provided by InteQ Corporation as part
 * of a InteQ Corporation product for use ONLY by licensed users of the product,
 * includes CONFIDENTIAL and PROPRIETARY information of InteQ Corporation.<br>
 * <br>
 * USE OF THIS SOFTWARE IS GOVERNED BY THE TERMS AND CONDITIONS
 * OF THE LICENSE STATEMENT AND LIMITED WARRANTY FURNISHED WITH
 * THE PRODUCT.<br>
 * <br>
 * </p>
 * -----------------------------------------------------------------------------
 * <br>
 * <br>
 ******************************************************************************/
package org.nextlevel.es.crypto;

/**
 * <p>This is a utility / helper class which provides helper functions for performing
 * byte operations such as comparing byte arrays, and converting byte arrays to strings.</p><br>
 *
 * @author  Ron Michaud
 * @version $Revision: 1.1.1.1 $
 * @since Platform 0.1
 */
public class ByteHandler
{
    
    /**
     * <p>Compares two byte arrays to check for equality. The comparison checks for equalty in terms of
     * the length as well as the contents of the byte arrays.</p><br>
     *
     * @param passwordX Byte array #1 that will be compared with byte array #2.
     * @param passwordY Byte array #2 that will be compared with byte array #1.
     *
     * @return  Returns a boolean flag indicating whether the two byte arrays are equal.<br>
     *          true - both the byte arrays are equal<br>
     *          false - both the byte arrays are not equal<br>
     */
    public static boolean isEqual(byte[] passwordX, byte[] passwordY)
    {
        if (passwordX == null || passwordY == null)
        {
            return false;
        }
        
        int lengthPasswordX = passwordX.length;
        int lengthPasswordY = passwordY.length;
        
        if (lengthPasswordX != lengthPasswordY)
        {
            return false;
        }
        else
        {
            for (int i = 0; i < lengthPasswordX; i++)
            {
                if (passwordX[i] != passwordY[i])
                {
                    return false;
                }
            }
        }
        
        return true;
    }
    
    /**
     * <p>Returns a string representation of the byte array.</p><br>
     *
     * @param byteArray Byte array that must be converted to a string.
     *
     * @return  String representation of the byte array passed.
     */
    public static String toString(byte[] byteArray)
    {
        StringBuffer result = new StringBuffer();
        
        for (int i = 0; i < byteArray.length; i++)
        {
            result.append(byteArray[i]);
        }
        
        return result.toString();
    }
}
