/*
 * Copyright (C) 2016-2017 Hamid Mushtaq
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package tudelft.utils.filemanagement;

import tudelft.utils.*;
import java.io.*;

public class FileManagerFactory
{
	public static FileManager createInstance(String type, Configuration config) throws IOException
	{
		if (type.equals("gpfs") || config.getMode() == "local")
			return new GPFSFileManager();
		else
			return new HDFSFileManager();
	}
}