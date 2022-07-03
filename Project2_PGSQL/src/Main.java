public class Main {
    public static void main(String[] args) {
        //Q1
        Center center = new Center();
        Enterprise enterprise = new Enterprise();
        Model model = new Model();
        Staff staff = new Staff();

//        testOperationOfCenter =====================================
//        center.insert(100, "cjy");
//
//        ArrayList<String> strs = new ArrayList<>();
//        strs.add("id");
//        strs.add("name");
//        center.select(strs);
//
//        center.modify(100, "cjy", 233, "cjynb");
//        center.delete(100, "cjynb");
//
//        center.select(strs);
//
//        System.out.println(Staff.isSupplyStaff.size());
//        testOperationOfCenter =====================================


//        Q2
        StockIn stockIn = new StockIn();

        //Q3
        PlaceOrder placeOrder = new PlaceOrder();

        // Q4
        placeOrder.updateOrder();

        // Q5
        placeOrder.deleteOrder();

        System.out.println("--------- the Q1 - Q5 finished ----------");

        // Q6
        staff.getAllStaffCount();

        // Q7
        placeOrder.getContractCount();

        // Q8
        placeOrder.getOrderCount();

        // Q9
        placeOrder.getNeverSoldProductCount();

        // Q10
        placeOrder.getFavoriteProductModel();

        // Q11
        stockIn.getAvgStockByCenter();

        // Q12
        stockIn.getProductByNumber("S03485W");
        stockIn.getProductByNumber("T28310Y");

        // Q13
        placeOrder.getContractInfo("CSE0000116");
        placeOrder.getContractInfo("CSE0000306");
    }
}
