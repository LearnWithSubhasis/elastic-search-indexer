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

/*******************************************************************************
 * <p>An exception that provides information on cryptography operation errors.
 * CryptographyException denotes a generic runtime cryptography operation
 * exception such as encryption, text matching etc. By declaring the exception
 * as a descendant of RuntimeException, the cryptographic helper classes gives
 * the programmer the option of whether or not to catch the exception.</p>
 *
 * @author  Sandeep Mankar
 * @version $Revision: 1.1.1.1 $
 * @since Platform 0.6
 *
 * @see com.inteqnet.core.crypto.EncryptionHelper
 ******************************************************************************/
public class CryptographyException extends RuntimeException
{
    /**
     * <p>Instance of the actual lower-level exception that had occurred.</p>
     */
    private Exception underlyingException;
    
    /**
     * <p>Exception Message</p>
     */
    private String message;
    
    /**
     * <p>Constructs an CryptographyException with a message.</p>
     *
     * @param msg Exception Message
     */
    public  CryptographyException(String msg)
    {
        this.message = msg;
    }
    
    /**
     * <p>Constructs an CryptographyException with a message and an lower-level
     * exception instance.</p>
     *
     * @param msg Exception Message
     * @param e Underlying Exception Object
     */
    public  CryptographyException(String msg, Exception e)
    {
        this.message = msg;
        this.underlyingException = e;
        
    }
    
    /**
     * <p>Returns the underlying cause of the CryptographyException so that it
     * can be dealt with specifically if needed.</p>
     *
     * @return Underlying cause of the CryptographyException
     */
    public Exception getUnderlyingException()
    {
        return this.underlyingException;
    }
    
    /**
     * <p>Returns the detail message string related to this exception.</p>
     *
     * @return String containing the detail message related to this exception.
     */
    public String getMessage()
    {
        return this.message;
    }
}
