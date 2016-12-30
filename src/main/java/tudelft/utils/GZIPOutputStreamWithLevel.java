/* SparkGA
 *
 * This file is a part of SparkGA.
 * 
 * Copyright (c) 2016-2017 TU Delft, The Netherlands.
 * All rights reserved.
 * 
 * SparkGA is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * SparkGA is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with SparkGA.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * Authors: Hamid Mushtaq
 *
*/
package utils;

import java.util.zip.GZIPOutputStream;
import java.io.OutputStream;
import java.io.IOException;

class GZIPOutputStreamWithLevel extends GZIPOutputStream 
{
	public GZIPOutputStreamWithLevel(OutputStream out) throws IOException 
	{
        super(out);
    } 
	
	public void setLevel(int level)
	{
		def.setLevel(level);
	}
}