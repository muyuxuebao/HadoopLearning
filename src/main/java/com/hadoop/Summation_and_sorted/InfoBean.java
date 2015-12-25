package com.hadoop.Summation_and_sorted;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by muyux on 2015/12/24.
 */
public class InfoBean implements WritableComparable<InfoBean> {
    private String account;
    private double income;
    private double expenses;
    private double surplus;


    public int compareTo(InfoBean o) {
        if (this.income != o.income) {
            return this.income > o.income ? -1 : 1;
        } else {
            return this.expenses < o.expenses ? -1 : 1;
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(account);
        out.writeDouble(income);
        out.writeDouble(expenses);
        out.writeDouble(surplus);
    }

    public void readFields(DataInput in) throws IOException {
        this.account = in.readUTF();
        this.income = in.readDouble();
        this.expenses = in.readDouble();
        this.surplus = in.readDouble();
    }

    public void set(String account, double income, double expenses) {
        this.account = account;
        this.income = income;
        this.expenses = expenses;
        this.surplus = income - expenses;
    }

    public double getSurplus() {
        return surplus;
    }

    @Override
    public String toString() {
        return income + "\t" + expenses + "\t" + surplus;
    }

    public void setSurplus(double surplus) {
        this.surplus = surplus;
    }

    public double getExpenses() {
        return expenses;
    }

    public void setExpenses(double expenses) {
        this.expenses = expenses;
    }

    public double getIncome() {
        return income;
    }

    public void setIncome(double income) {
        this.income = income;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }
}
