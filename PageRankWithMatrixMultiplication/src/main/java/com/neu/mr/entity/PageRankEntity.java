/**
 * 
 */
package com.neu.mr.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author harsha
 *
 *	Data structure to hold the page information. It holds,
 *	- Page name
 *	- Page Rank
 *	- Number of Outlinks
 *	- List of Outlinks from this page.
 */
public class PageRankEntity implements Writable, WritableComparable<PageRankEntity> {
	
	private Text pageName;
	
	private DoubleWritable pageRank;
	
	private IntWritable outlinkSize;
	
	private List<Text> outlinks;
	
	public PageRankEntity() {
		this.pageName = new Text("");
		this.pageRank = new DoubleWritable(0.0);
		this.outlinkSize = new IntWritable(0);
		this.outlinks = new ArrayList<Text>();
	}

	public PageRankEntity(Text pageName, IntWritable outlinkSize, List<Text> outlinks) {
		this.pageName = pageName;
		this.pageRank = new DoubleWritable(0.0);
		this.outlinkSize = outlinkSize;
		this.outlinks = outlinks;
	}

	public PageRankEntity(Text pageName, DoubleWritable pageRank, IntWritable outlinkSize, List<Text> outlinks) {
		this.pageName = pageName;
		this.pageRank = pageRank;
		this.outlinkSize = outlinkSize;
		this.outlinks = outlinks;
	}

	public PageRankEntity(Text pageName) {
		this.pageName = pageName;
		this.pageRank = new DoubleWritable(0.0);
		this.outlinkSize = new IntWritable(0);
		this.outlinks = new ArrayList<Text>();
	}

	public PageRankEntity(Text pageName, DoubleWritable pageRank) {
		this.pageName = pageName;
		this.pageRank = pageRank;
		this.outlinkSize = new IntWritable(0);
		this.outlinks = new ArrayList<Text>();
	}
	
	/**
	 * @return the pageName
	 */
	public String getPageName() {
		return pageName.toString();
	}

	/**
	 * @param pageName the pageName to set
	 */
	public void setPageName(Text pageName) {
		this.pageName = pageName;
	}

	/**
	 * @return the pageRank
	 */
	public DoubleWritable getPageRank() {
		return pageRank;
	}

	/**
	 * @param pageRank the pageRank to set
	 */
	public void setPageRank(DoubleWritable pageRank) {
		this.pageRank = pageRank;
	}

	/**
	 * @return the outlinkSize
	 */
	public int getOutlinkSize() {
		return outlinkSize.get();
	}

	/**
	 * @param outlinkSize the outlinkSize to set
	 */
	public void setOutlinkSize(IntWritable outlinkSize) {
		this.outlinkSize = outlinkSize;
	}

	/**
	 * @return the outlinks
	 */
	public List<Text> getOutlinks() {
		return outlinks;
	}

	/**
	 * @param outlinks the outlinks to set
	 */
	public void setOutlinks(List<Text> outlinks) {
		this.outlinks = outlinks;
	}
	
	

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(getPageName() + "~");
		stringBuilder.append(getPageRank() + "~");
		stringBuilder.append(getOutlinkSize() + "~");
		for(Text outlink : getOutlinks()){
			stringBuilder.append(outlink.toString() + "~");
		}
		return stringBuilder.toString();
	}
	
	public String toStringForTopK() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(getPageName() + " : ");
		stringBuilder.append(getPageRank());
		return stringBuilder.toString();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		this.pageName.write(out);
		this.pageRank.write(out);
		this.outlinkSize.write(out);
		for(Text outlink : outlinks){
			outlink.write(out);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.pageName.readFields(in);
		this.pageRank.readFields(in);
		this.outlinkSize.readFields(in);
		this.outlinks = new ArrayList<Text>();
		for(int i = 0; i < this.outlinkSize.get(); i++){
			Text outlink = new Text("");
			outlink.readFields(in);
			if(!outlink.equals(this.pageName)
					&& !outlinks.contains(outlink)){
				this.outlinks.add(outlink);
			}
		}
		this.outlinkSize = new IntWritable(this.outlinks.size());
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(PageRankEntity pageRankEntity) {
		return this.pageName.toString().compareTo(pageRankEntity.getPageName().toString());
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((outlinkSize == null) ? 0 : outlinkSize.hashCode());
		result = prime * result + ((outlinks == null) ? 0 : outlinks.hashCode());
		result = prime * result + ((pageName == null) ? 0 : pageName.hashCode());
		result = prime * result + ((pageRank == null) ? 0 : pageRank.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PageRankEntity other = (PageRankEntity) obj;
		if (outlinkSize == null) {
			if (other.outlinkSize != null)
				return false;
		} else if (!outlinkSize.equals(other.outlinkSize))
			return false;
		if (outlinks == null) {
			if (other.outlinks != null)
				return false;
		} else if (!outlinks.equals(other.outlinks))
			return false;
		if (pageName == null) {
			if (other.pageName != null)
				return false;
		} else if (!pageName.equals(other.pageName))
			return false;
		if (pageRank == null) {
			if (other.pageRank != null)
				return false;
		} else if (!pageRank.equals(other.pageRank))
			return false;
		return true;
	}

}
