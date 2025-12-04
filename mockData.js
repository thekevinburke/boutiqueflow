const mockReceipts = [
    {
      id: 'REC-001',
      date: '2024-12-02',
      vendor: 'Queen of Sparkles',
      poNumber: 'PO-2024-0892',
      itemCount: 4,
      status: 'new',
      items: [
        { id: 'ITEM-001', name: 'Hot Pink NYE Icon Button-Up Cardigan', color: 'Hot Pink', size: 'S, M, L', category: 'Sweaters', status: 'new' },
        { id: 'ITEM-002', name: 'Champagne Toast Sequin Top', color: 'Gold', size: 'S, M, L, XL', category: 'Tops', status: 'new' },
        { id: 'ITEM-003', name: 'Midnight Kiss Sparkle Dress', color: 'Black', size: 'S, M, L', category: 'Dresses', status: 'new' },
        { id: 'ITEM-004', name: 'Confetti Pop Cardigan', color: 'Ivory Multi', size: 'S, M, L', category: 'Sweaters', status: 'new' },
      ]
    },
    {
      id: 'REC-002',
      date: '2024-12-01',
      vendor: 'Free People',
      poNumber: 'PO-2024-0885',
      itemCount: 3,
      status: 'in_progress',
      items: [
        { id: 'ITEM-005', name: 'Cozy Cloud Pullover', color: 'Oatmeal', size: 'XS, S, M, L', category: 'Sweaters', status: 'completed' },
        { id: 'ITEM-006', name: 'Velvet Dreams Blazer', color: 'Burgundy', size: 'S, M, L', category: 'Jackets', status: 'in_progress' },
        { id: 'ITEM-007', name: 'Bohemian Nights Maxi Dress', color: 'Forest Green', size: 'S, M, L', category: 'Dresses', status: 'new' },
      ]
    },
    {
      id: 'REC-003',
      date: '2024-11-29',
      vendor: 'Show Me Your Mumu',
      poNumber: 'PO-2024-0879',
      itemCount: 2,
      status: 'completed',
      items: [
        { id: 'ITEM-008', name: 'Party Sequin Mini Skirt', color: 'Silver', size: 'S, M, L', category: 'Skirts', status: 'completed' },
        { id: 'ITEM-009', name: 'Satin Wrap Blouse', color: 'Champagne', size: 'S, M, L, XL', category: 'Tops', status: 'completed' },
      ]
    },
    {
      id: 'REC-004',
      date: '2024-11-28',
      vendor: 'ASTR The Label',
      poNumber: 'PO-2024-0871',
      itemCount: 5,
      status: 'new',
      items: [
        { id: 'ITEM-010', name: 'Pleated Midi Skirt', color: 'Black', size: 'S, M, L', category: 'Skirts', status: 'new' },
        { id: 'ITEM-011', name: 'Ruched Bodycon Dress', color: 'Red', size: 'XS, S, M, L', category: 'Dresses', status: 'new' },
        { id: 'ITEM-012', name: 'Oversized Blazer', color: 'Camel', size: 'S, M, L', category: 'Jackets', status: 'new' },
        { id: 'ITEM-013', name: 'Silk Camisole', color: 'Ivory', size: 'S, M, L', category: 'Tops', status: 'new' },
        { id: 'ITEM-014', name: 'Wide Leg Trousers', color: 'Navy', size: 'S, M, L', category: 'Pants', status: 'new' },
      ]
    },
  ];
  
  module.exports = mockReceipts;