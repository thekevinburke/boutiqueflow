const express = require('express');
const multer = require('multer');
const Anthropic = require('@anthropic-ai/sdk').default;
const path = require('path');
const { Pool } = require('pg');
require('dotenv').config();

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('railway') ? { rejectUnauthorized: false } : false,
});

// Initialize database tables
async function initDatabase() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS processed_items (
        id SERIAL PRIMARY KEY,
        item_type VARCHAR(10) NOT NULL,
        heartland_id VARCHAR(50) NOT NULL,
        receipt_id VARCHAR(50),
        status VARCHAR(20) NOT NULL DEFAULT 'new',
        processed_at TIMESTAMP,
        processed_by VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(item_type, heartland_id)
      )
    `);
    
    // Inventory analysis cache table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS inventory_cache (
        id SERIAL PRIMARY KEY,
        cache_key VARCHAR(50) NOT NULL UNIQUE,
        data JSONB NOT NULL,
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    console.log('Database initialized successfully');
  } catch (error) {
    console.error('Error initializing database:', error);
  }
}

// Initialize DB on startup
initDatabase();

// Basic Auth middleware
const USERS = {
  'adminkevin': 'Monkees842',
  // Add more users here as needed, e.g.:
  // 'testuser': 'testpassword',
};

function basicAuth(req, res, next) {
  // Skip auth for health check endpoint
  if (req.path === '/api/health') {
    return next();
  }

  const authHeader = req.headers.authorization;

  if (!authHeader || !authHeader.startsWith('Basic ')) {
    res.setHeader('WWW-Authenticate', 'Basic realm="BoutiqueFlow"');
    return res.status(401).send('Authentication required');
  }

  const base64Credentials = authHeader.split(' ')[1];
  const credentials = Buffer.from(base64Credentials, 'base64').toString('ascii');
  const [username, password] = credentials.split(':');

  if (USERS[username] && USERS[username] === password) {
    req.username = username; // Store username for tracking who processed items
    return next();
  }

  res.setHeader('WWW-Authenticate', 'Basic realm="BoutiqueFlow"');
  return res.status(401).send('Invalid credentials');
}

// Apply Basic Auth to all routes
app.use(basicAuth);

const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

// Heartland API configuration
const HEARTLAND_BASE_URL = `https://${process.env.HEARTLAND_SUBDOMAIN}.retail.heartland.us/api`;
const HEARTLAND_TOKEN = process.env.HEARTLAND_API_TOKEN;

// Helper function to make Heartland API requests
async function heartlandRequest(endpoint, options = {}) {
  const url = `${HEARTLAND_BASE_URL}${endpoint}`;
  const response = await fetch(url, {
    ...options,
    headers: {
      'Authorization': `Bearer ${HEARTLAND_TOKEN}`,
      'Content-Type': 'application/json',
      ...options.headers,
    },
  });
  
  if (!response.ok) {
    // Try to get more details from the response
    let errorDetails = '';
    try {
      const errorBody = await response.text();
      errorDetails = ` - ${errorBody}`;
      console.error('Heartland API error details:', errorBody);
    } catch (e) {
      // Ignore if we can't read the body
    }
    throw new Error(`Heartland API error: ${response.status} ${response.statusText}${errorDetails}`);
  }
  
  // Handle empty responses (common for PUT/DELETE)
  const text = await response.text();
  if (!text) {
    return { success: true };
  }
  
  try {
    return JSON.parse(text);
  } catch (e) {
    // If it's not JSON, return the text
    return { success: true, body: text };
  }
}

// Helper function to get item status from database
async function getItemStatus(itemType, heartlandId) {
  try {
    const result = await pool.query(
      'SELECT status FROM processed_items WHERE item_type = $1 AND heartland_id = $2',
      [itemType, heartlandId.toString()]
    );
    return result.rows[0]?.status || 'new';
  } catch (error) {
    console.error('Error getting item status:', error);
    return 'new';
  }
}

// Helper function to update item status in database
async function updateItemStatus(itemType, heartlandId, status, receiptId = null, username = null) {
  try {
    await pool.query(`
      INSERT INTO processed_items (item_type, heartland_id, receipt_id, status, processed_at, processed_by)
      VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, $5)
      ON CONFLICT (item_type, heartland_id)
      DO UPDATE SET status = $4, receipt_id = $3, processed_at = CURRENT_TIMESTAMP, processed_by = $5, updated_at = CURRENT_TIMESTAMP
    `, [itemType, heartlandId.toString(), receiptId, status, username]);
    return true;
  } catch (error) {
    console.error('Error updating item status:', error);
    return false;
  }
}

app.use(express.json());
app.use(express.static('public'));

// Generate product description from images
app.post('/api/generate', upload.array('images', 10), async (req, res) => {
  try {
    const { productName, vendor, color, category, vendorDescription } = req.body;
    const images = req.files;

    if (!images || images.length === 0) {
      return res.status(400).json({ error: 'At least one image is required' });
    }

    // Prepare image content for Claude
    const imageContent = images.map(img => ({
      type: 'image',
      source: {
        type: 'base64',
        media_type: img.mimetype,
        data: img.buffer.toString('base64'),
      },
    }));

    // Build the prompt
    const prompt = `You are a copywriter for Monkee's of Chattanooga, an upscale women's boutique known for stylish, curated fashion. Write a product description for our Shopify store.

Product Details:
- Product Name: ${productName || 'Not provided'}
- Vendor/Brand: ${vendor || 'Not provided'}
- Color: ${color || 'Not provided'}
- Category: ${category || 'Not provided'}
${vendorDescription ? `- Vendor's Description: ${vendorDescription}` : ''}

Based on the product images and details above, write a product description following this exact structure:

**OPENING (2-3 sentences):**
Start with an engaging hook that captures the item's appeal and vibe. Mention the brand name and product name naturally. Describe what makes this piece special and when/where to wear it.

**FEATURES (5-6 bullet points, one per line):**
Each bullet should be on its own line and cover:
- Key design details and embellishments visible in the images
- Fabric/material if known or visible
- Fit and silhouette (relaxed, fitted, oversized, etc.)
- Color and any accent colors or patterns
- Styling suggestions (what to pair it with)
- Care instructions if mentioned in vendor description, otherwise note "See label for care instructions"

Keep the tone warm, sophisticated, and aspirational—like a trusted friend who works in fashion giving advice. Avoid over-the-top phrases like "isn't just a [item]—it's a statement" or "elevate your wardrobe." Be genuine and specific.

Total length should be 150-250 words.

**META DESCRIPTION:**
Write an SEO-friendly meta description under 160 characters. Include the brand name, product type, and one key appeal (like the occasion, season, or standout feature).

Format your response exactly like this:
---DESCRIPTION---
[Opening sentences]

[Bullet points, each on its own line starting with •]

---META---
[Meta description]

---END---`;

    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1024,
      messages: [
        {
          role: 'user',
          content: [
            ...imageContent,
            { type: 'text', text: prompt },
          ],
        },
      ],
    });

    const responseText = response.content[0].text;

    // Parse the response
    const descriptionMatch = responseText.match(/---DESCRIPTION---([\s\S]*?)---META---/);
    const metaMatch = responseText.match(/---META---([\s\S]*?)---END---/);

    const descriptionBase = descriptionMatch ? descriptionMatch[1].trim() : responseText;
    
    // Convert to HTML format for Shopify
    const htmlDescription = convertToHtml(descriptionBase);
    
    const metaDescription = metaMatch ? metaMatch[1].trim() : '';

    res.json({
      success: true,
      description: htmlDescription,
      metaDescription,
      raw: responseText,
    });

  } catch (error) {
    console.error('Error generating description:', error);
    res.status(500).json({ error: error.message });
  }
});

// Convert plain text description to HTML for Shopify
function convertToHtml(text) {
  const lines = text.split('\n').filter(line => line.trim());
  let html = '';
  let inList = false;
  
  for (const line of lines) {
    const trimmed = line.trim();
    
    // Check if it's a bullet point
    if (trimmed.startsWith('•') || trimmed.startsWith('-') || trimmed.startsWith('*')) {
      if (!inList) {
        html += '<ul>\n';
        inList = true;
      }
      // Remove the bullet character and trim
      const content = trimmed.replace(/^[•\-\*]\s*/, '');
      html += `<li>${content}</li>\n`;
    } else {
      // It's a paragraph
      if (inList) {
        html += '</ul>\n';
        inList = false;
      }
      if (trimmed) {
        html += `<p>${trimmed}</p>\n`;
      }
    }
  }
  
  // Close list if still open
  if (inList) {
    html += '</ul>\n';
  }
  
  // Add the CTA footer
  html += '<p><strong>Not sure of the fit? Need more information?</strong></p>\n';
  html += '<p><strong>We\'re here to help! Send us a DM @monkeesofchattanooga or call 423-486-1300!</strong></p>';
  
  return html;
}

// Cache for vendor names (to avoid repeated API calls)
const vendorCache = new Map();

async function getVendorName(vendorId) {
  if (!vendorId) return 'Unknown Vendor';
  
  if (vendorCache.has(vendorId)) {
    return vendorCache.get(vendorId);
  }
  
  try {
    const vendor = await heartlandRequest(`/purchasing/vendors/${vendorId}`);
    const name = vendor.name || 'Unknown Vendor';
    vendorCache.set(vendorId, name);
    return name;
  } catch (error) {
    console.error(`Error fetching vendor ${vendorId}:`, error);
    return 'Unknown Vendor';
  }
}

// Get all receipts from Heartland
app.get('/api/receipts', async (req, res) => {
  try {
    // Fetch recent COMPLETE receipts (last 30 days, limit 50), sorted by updated_at descending (newest first)
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
    const dateFilter = thirtyDaysAgo.toISOString().split('T')[0];
    
    const data = await heartlandRequest(`/purchasing/receipts?per_page=50&_filter[created_at][$gte]=${dateFilter}&_filter[status]=complete&sort[]=updated_at,desc`);
    
    // Get all receipt IDs to batch query statuses
    const receiptIds = data.results.map(r => r.id.toString());
    
    // Batch query all statuses from database
    let statusMap = {};
    try {
      const statusResult = await pool.query(
        `SELECT receipt_id, status, COUNT(*) as count 
         FROM processed_items 
         WHERE receipt_id = ANY($1::text[])
         GROUP BY receipt_id, status`,
        [receiptIds]
      );
      
      for (const row of statusResult.rows) {
        if (!statusMap[row.receipt_id]) {
          statusMap[row.receipt_id] = { completed: 0, skipped: 0 };
        }
        if (row.status === 'completed') statusMap[row.receipt_id].completed = parseInt(row.count);
        if (row.status === 'skipped') statusMap[row.receipt_id].skipped = parseInt(row.count);
      }
    } catch (e) {
      console.error('Error batch querying statuses:', e);
    }
    
    // Process receipts
    const receipts = [];
    for (const r of data.results) {
      let vendorName = 'Unknown Vendor';
      let itemCount = 0;
      let gridCount = 0;
      
      // Get lines and extract vendor from first item
      try {
        const lines = await heartlandRequest(`/purchasing/receipts/${r.id}/lines?per_page=100`);
        itemCount = lines.total || 0;
        
        // Count unique grids
        const gridIds = new Set();
        for (const line of lines.results || []) {
          if (line.grid_id) {
            gridIds.add(line.grid_id);
          }
        }
        
        // Get vendor from first item's primary_vendor_id (and grid_id if not found in lines)
        if (lines.results && lines.results.length > 0) {
          const firstLine = lines.results[0];
          if (firstLine.item_id) {
            try {
              const item = await heartlandRequest(`/items/${firstLine.item_id}`);
              if (item.primary_vendor_id) {
                vendorName = await getVendorName(item.primary_vendor_id);
              }
              // If no grid_ids found in lines, get from item
              if (gridIds.size === 0 && item.grid_id) {
                gridIds.add(item.grid_id);
              }
            } catch (e) {
              console.error('Error fetching item for vendor:', e);
            }
          }
        }
        
        gridCount = gridIds.size || 1; // Default to 1 if still no grids found
      } catch (e) {
        console.error('Error fetching receipt lines:', e);
      }
      
      // Calculate receipt status from pre-fetched data
      let receiptStatus = 'new';
      const stats = statusMap[r.id.toString()];
      if (stats) {
        const done = stats.completed + stats.skipped;
        if (done > 0) {
          if (done >= gridCount) {
            receiptStatus = 'completed';
          } else {
            receiptStatus = 'in_progress';
          }
        }
      }
      
      receipts.push({
        id: `REC-${r.id}`,
        heartlandId: r.id,
        date: r.updated_at ? r.updated_at.split('T')[0] : (r.created_at ? r.created_at.split('T')[0] : new Date().toISOString().split('T')[0]),
        vendor: vendorName,
        receiptNumber: r.public_id || `${r.id}`,
        itemCount: itemCount,
        gridCount: gridCount,
        status: receiptStatus,
      });
    }
    
    res.json(receipts);
  } catch (error) {
    console.error('Error fetching receipts from Heartland:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get single receipt with items grouped by grid
app.get('/api/receipts/:id', async (req, res) => {
  try {
    // Extract Heartland ID from our ID format (REC-123 -> 123)
    const heartlandId = req.params.id.replace('REC-', '');
    
    const receipt = await heartlandRequest(`/purchasing/receipts/${heartlandId}`);
    const linesData = await heartlandRequest(`/purchasing/receipts/${heartlandId}/lines?per_page=100`);
    
    let vendorName = 'Unknown Vendor';
    
    // Fetch item details for each line
    const rawItems = [];
    for (const line of linesData.results) {
      let itemDetails = {
        description: 'Unknown Item',
        custom: {},
        grid_id: null
      };
      
      try {
        itemDetails = await heartlandRequest(`/items/${line.item_id}`);
        
        // Get vendor from first item's primary_vendor_id (set once for the receipt)
        if (vendorName === 'Unknown Vendor' && itemDetails.primary_vendor_id) {
          vendorName = await getVendorName(itemDetails.primary_vendor_id);
        }
      } catch (e) {
        console.error(`Error fetching item ${line.item_id}:`, e);
      }
      
      rawItems.push({
        heartlandItemId: line.item_id,
        heartlandLineId: line.id,
        name: itemDetails.description || 'Unknown Item',
        colorName: itemDetails.custom?.color_name || itemDetails.custom?.Color_Name || itemDetails.custom?.color || itemDetails.custom?.Color || '',
        size: itemDetails.custom?.size || itemDetails.custom?.Size || '',
        category: itemDetails.custom?.category || itemDetails.custom?.Category || itemDetails.custom?.department || '',
        styleName: itemDetails.custom?.style_name || itemDetails.custom?.Style_Name || '',
        gridId: itemDetails.grid_id || null,
        longDescription: itemDetails.long_description || '',
        qty: line.qty,
        unitCost: line.unit_cost,
      });
    }
    
    // Group items by grid_id (null grid_id = standalone item)
    const gridGroups = new Map();
    const standaloneItems = [];
    
    for (const item of rawItems) {
      if (item.gridId) {
        if (!gridGroups.has(item.gridId)) {
          gridGroups.set(item.gridId, {
            gridId: item.gridId,
            styleName: item.styleName || item.name.split(' - ')[0], // Use style name or first part of description
            category: item.category,
            longDescription: item.longDescription,
            variants: [],
            colors: new Set(),
            sizes: new Set(),
          });
        }
        const group = gridGroups.get(item.gridId);
        group.variants.push(item);
        if (item.colorName) group.colors.add(item.colorName);
        if (item.size) group.sizes.add(item.size);
      } else {
        standaloneItems.push(item);
      }
    }
    
    // Convert grid groups to array format for frontend
    const items = [];
    
    // Add grid groups
    for (const [gridId, group] of gridGroups) {
      // Get status from database
      const status = await getItemStatus('grid', gridId);
      
      items.push({
        id: `GRID-${gridId}`,
        type: 'grid',
        gridId: gridId,
        name: group.styleName,
        category: group.category,
        colors: Array.from(group.colors).sort(),
        sizes: Array.from(group.sizes).sort(),
        variantCount: group.variants.length,
        longDescription: group.longDescription,
        status: status,
        // Include first variant's item ID for fetching additional details if needed
        heartlandItemId: group.variants[0]?.heartlandItemId,
      });
    }
    
    // Add standalone items
    for (const item of standaloneItems) {
      // Get status from database
      const status = await getItemStatus('item', item.heartlandItemId);
      
      items.push({
        id: `ITEM-${item.heartlandLineId}`,
        type: 'item',
        heartlandItemId: item.heartlandItemId,
        name: item.name,
        category: item.category,
        colors: item.colorName ? [item.colorName] : [],
        sizes: item.size ? [item.size] : [],
        variantCount: 1,
        longDescription: item.longDescription,
        status: status,
      });
    }
    
    res.json({
      id: req.params.id,
      heartlandId: receipt.id,
      date: receipt.created_at ? receipt.created_at.split('T')[0] : new Date().toISOString().split('T')[0],
      vendor: vendorName,
      receiptNumber: receipt.public_id || `${receipt.id}`,
      itemCount: rawItems.length,
      productCount: items.length, // Number of unique products (grids + standalone)
      status: 'new',
      items: items,
    });
  } catch (error) {
    console.error('Error fetching receipt from Heartland:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get single item
app.get('/api/items/:id', async (req, res) => {
  try {
    // Extract Heartland line ID from our ID format (ITEM-123 -> 123)
    const lineId = req.params.id.replace('ITEM-', '');
    
    const itemId = req.query.itemId;
    
    if (itemId) {
      const item = await heartlandRequest(`/items/${itemId}`);
      
      // Try to get vendor from primary_vendor_id
      let vendorName = 'Unknown Vendor';
      if (item.primary_vendor_id) {
        vendorName = await getVendorName(item.primary_vendor_id);
      }
      
      // Get status from database
      const status = await getItemStatus('item', itemId);
      
      res.json({
        id: req.params.id,
        type: 'item',
        heartlandItemId: itemId,
        name: item.description || 'Unknown Item',
        colors: [item.custom?.color_name || item.custom?.Color_Name || item.custom?.color || item.custom?.Color || ''].filter(c => c),
        sizes: [item.custom?.size || item.custom?.Size || ''].filter(s => s),
        category: item.custom?.category || item.custom?.Category || item.custom?.department || '',
        vendor: vendorName,
        status: status,
        longDescription: item.long_description || '',
        price: item.price,
        cost: item.cost,
      });
    } else {
      // If no itemId provided, return error
      res.status(400).json({ error: 'itemId query parameter required' });
    }
  } catch (error) {
    console.error('Error fetching item from Heartland:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get single grid
app.get('/api/grids/:id', async (req, res) => {
  try {
    const gridId = req.params.id.replace('GRID-', '');
    
    const grid = await heartlandRequest(`/item_grids/${gridId}`);
    
    // Get vendor from grid's primary_vendor_id
    let vendorName = 'Unknown Vendor';
    if (grid.item_primary_vendor_id) {
      vendorName = await getVendorName(grid.item_primary_vendor_id);
    }
    
    // Get items in this grid to show color/size info and get style_name/category
    const itemsData = await heartlandRequest(`/items?_filter[grid_id]=${gridId}&per_page=50`);
    
    const colors = new Set();
    const sizes = new Set();
    let styleName = '';
    let category = '';
    
    for (const item of itemsData.results || []) {
      const colorName = item.custom?.color_name || item.custom?.Color_Name || item.custom?.color || item.custom?.Color || '';
      const size = item.custom?.size || item.custom?.Size || '';
      if (colorName) colors.add(colorName);
      if (size) sizes.add(size);
      
      // Get style_name and category from first item that has them
      if (!styleName) {
        styleName = item.custom?.style_name || item.custom?.Style_Name || '';
      }
      if (!category) {
        category = item.custom?.category || item.custom?.Category || item.custom?.department || item.custom?.Department || '';
      }
    }
    
    // Get status from database
    const status = await getItemStatus('grid', gridId);
    
    // Use item_description from grid, fall back to style_name from items, then 'Unknown Grid'
    const gridName = grid.item_description || styleName || 'Unknown Grid';
    
    res.json({
      id: `GRID-${gridId}`,
      type: 'grid',
      gridId: gridId,
      name: gridName,
      colors: Array.from(colors).sort(),
      sizes: Array.from(sizes).sort(),
      variantCount: itemsData.total || 0,
      category: grid.custom?.category || grid.custom?.Category || category || '',
      vendor: vendorName,
      status: status,
      longDescription: grid.long_description || grid.item_long_description || '',
      price: grid.item_price,
      cost: grid.item_cost,
    });
  } catch (error) {
    console.error('Error fetching grid from Heartland:', error);
    res.status(500).json({ error: error.message });
  }
});

// Update item description in Heartland and mark as completed
app.put('/api/items/:id', async (req, res) => {
  try {
    const itemId = req.query.itemId || req.params.id.replace('ITEM-', '');
    const { longDescription, status, receiptId } = req.body;
    
    // If there's a description, update Heartland
    if (longDescription !== undefined) {
      await heartlandRequest(`/items/${itemId}`, {
        method: 'PUT',
        body: JSON.stringify({
          long_description: longDescription,
        }),
      });
    }
    
    // Update status in database (include receiptId for status tracking)
    const newStatus = status || 'completed';
    const cleanReceiptId = receiptId ? receiptId.replace('REC-', '') : null;
    await updateItemStatus('item', itemId, newStatus, cleanReceiptId, req.username);
    
    res.json({ success: true, status: newStatus });
  } catch (error) {
    console.error('Error updating item in Heartland:', error);
    res.status(500).json({ error: error.message });
  }
});

// Update grid description in Heartland and mark as completed
app.put('/api/grids/:id', async (req, res) => {
  try {
    const gridId = req.params.id.replace('GRID-', '');
    const { longDescription, status, receiptId } = req.body;
    
    // If there's a description, update Heartland
    if (longDescription !== undefined) {
      await heartlandRequest(`/item_grids/${gridId}`, {
        method: 'PUT',
        body: JSON.stringify({
          long_description: longDescription,
        }),
      });
    }
    
    // Update status in database (include receiptId for status tracking)
    const newStatus = status || 'completed';
    const cleanReceiptId = receiptId ? receiptId.replace('REC-', '') : null;
    await updateItemStatus('grid', gridId, newStatus, cleanReceiptId, req.username);
    
    res.json({ success: true, status: newStatus });
  } catch (error) {
    console.error('Error updating grid in Heartland:', error);
    res.status(500).json({ error: error.message });
  }
});

// Skip an item (mark as skipped without updating Heartland)
app.post('/api/items/:id/skip', async (req, res) => {
  try {
    const itemId = req.query.itemId || req.params.id.replace('ITEM-', '');
    await updateItemStatus('item', itemId, 'skipped', null, req.username);
    res.json({ success: true, status: 'skipped' });
  } catch (error) {
    console.error('Error skipping item:', error);
    res.status(500).json({ error: error.message });
  }
});

// Skip a grid (mark as skipped without updating Heartland)
app.post('/api/grids/:id/skip', async (req, res) => {
  try {
    const gridId = req.params.id.replace('GRID-', '');
    await updateItemStatus('grid', gridId, 'skipped', null, req.username);
    res.json({ success: true, status: 'skipped' });
  } catch (error) {
    console.error('Error skipping grid:', error);
    res.status(500).json({ error: error.message });
  }
});

// ==================== INVENTORY IQ ENDPOINTS ====================

// Get cached inventory data (fast read)
app.get('/api/inventory/data', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT data, synced_at FROM inventory_cache WHERE cache_key = 'inventory_analysis'`
    );
    
    if (result.rows.length === 0) {
      return res.json({ 
        error: 'No data yet', 
        message: 'Run a sync first',
        syncedAt: null 
      });
    }
    
    const { data, synced_at } = result.rows[0];
    res.json({
      ...data,
      syncedAt: synced_at
    });
  } catch (error) {
    console.error('Error reading inventory cache:', error);
    res.status(500).json({ error: error.message });
  }
});

// Sync inventory data (heavy calculation - run nightly or manually)
app.post('/api/inventory/sync', async (req, res) => {
  try {
    console.log('Starting inventory sync...');
    const startTime = Date.now();
    
    // ========== STEP 1: Get all receipts and build item receive dates ==========
    console.log('Fetching receipts...');
    const oneYearAgo = new Date(Date.now() - 365*24*60*60*1000).toISOString().split('T')[0];
    const receiptsData = await heartlandRequest(`/purchasing/receipts?per_page=200&_filter[created_at][$gte]=${oneYearAgo}&_filter[status]=complete`);
    console.log(`Found ${receiptsData.results?.length || 0} receipts`);
    
    // Map item_id -> { receiveDate, vendor_id }
    const itemReceiveInfo = {};
    for (const receipt of receiptsData.results || []) {
      try {
        const lines = await heartlandRequest(`/purchasing/receipts/${receipt.id}/lines?per_page=100`);
        const receiveDate = receipt.completed_at || receipt.created_at;
        for (const line of lines.results || []) {
          if (line.item_id) {
            if (!itemReceiveInfo[line.item_id] || receiveDate < itemReceiveInfo[line.item_id].receiveDate) {
              itemReceiveInfo[line.item_id] = {
                receiveDate: receiveDate,
                category: line.item_custom?.category || line.item_custom?.Category || '',
                vendor: null // Will fill in later
              };
            }
          }
        }
      } catch (e) {
        console.error(`Error fetching receipt ${receipt.id} lines:`, e.message);
      }
    }
    console.log(`Mapped ${Object.keys(itemReceiveInfo).length} items to receive dates`);
    
    // ========== STEP 2: Get all sales data ==========
    console.log('Fetching sales data...');
    const salesData = await heartlandRequest('/reporting/sales?per_page=1000');
    console.log(`Found ${salesData.results?.length || 0} sales records`);
    
    // Map item_id -> array of sale records
    const itemSales = {};
    for (const sale of salesData.results || []) {
      if (sale.item_id && sale.net_qty_sold > 0) {
        if (!itemSales[sale.item_id]) {
          itemSales[sale.item_id] = [];
        }
        itemSales[sale.item_id].push({
          date: sale.datetime,
          qty: sale.net_qty_sold,
          revenue: sale.net_sales
        });
      }
    }
    
    // ========== STEP 3: Get current inventory ==========
    console.log('Fetching inventory...');
    const inventoryData = await heartlandRequest('/inventory/values?group[]=item_id&per_page=500');
    console.log(`Found ${inventoryData.results?.length || 0} inventory items`);
    
    // ========== STEP 4: Get item details for categorization ==========
    console.log('Fetching item details...');
    const itemDetails = {};
    const itemIds = new Set([
      ...Object.keys(itemReceiveInfo),
      ...Object.keys(itemSales),
      ...(inventoryData.results || []).map(i => i.item_id?.toString()).filter(Boolean)
    ]);
    
    let itemCount = 0;
    for (const itemId of itemIds) {
      if (itemCount >= 300) break; // Limit to avoid timeout
      try {
        const item = await heartlandRequest(`/items/${itemId}`);
        let vendorName = 'Unknown';
        if (item.primary_vendor_id) {
          vendorName = await getVendorName(item.primary_vendor_id);
        }
        itemDetails[itemId] = {
          name: item.custom?.style_name || item.description || 'Unknown',
          category: item.custom?.category || item.custom?.Category || 'Uncategorized',
          vendor: vendorName,
          cost: item.cost || 0,
          price: item.price || 0
        };
        itemCount++;
      } catch (e) {
        // Skip items that can't be fetched
      }
    }
    console.log(`Fetched details for ${itemCount} items`);
    
    // ========== STEP 5: Calculate sell-through velocity ==========
    const now = new Date();
    const velocityByCategory = {};
    const velocityByVendor = {};
    
    // For each item that has both receive date and sales
    for (const [itemId, sales] of Object.entries(itemSales)) {
      const receiveInfo = itemReceiveInfo[itemId];
      const details = itemDetails[itemId];
      
      if (!receiveInfo || !details) continue;
      
      const receiveDate = new Date(receiveInfo.receiveDate);
      
      // Calculate days to first sale
      const firstSale = sales.sort((a, b) => new Date(a.date) - new Date(b.date))[0];
      const daysToSell = Math.floor((new Date(firstSale.date) - receiveDate) / (1000 * 60 * 60 * 24));
      
      if (daysToSell < 0 || daysToSell > 365) continue; // Skip bad data
      
      const category = details.category;
      const vendor = details.vendor;
      
      // Aggregate by category
      if (!velocityByCategory[category]) {
        velocityByCategory[category] = { totalDays: 0, count: 0, items: [] };
      }
      velocityByCategory[category].totalDays += daysToSell;
      velocityByCategory[category].count++;
      
      // Aggregate by vendor
      if (!velocityByVendor[vendor]) {
        velocityByVendor[vendor] = { totalDays: 0, count: 0, items: [] };
      }
      velocityByVendor[vendor].totalDays += daysToSell;
      velocityByVendor[vendor].count++;
    }
    
    // Calculate averages and format
    const categoryVelocity = Object.entries(velocityByCategory)
      .map(([name, data]) => ({
        name,
        avgDaysToSell: Math.round(data.totalDays / data.count),
        itemsSold: data.count
      }))
      .filter(c => c.itemsSold >= 3) // Only include categories with enough data
      .sort((a, b) => a.avgDaysToSell - b.avgDaysToSell);
    
    const vendorVelocity = Object.entries(velocityByVendor)
      .map(([name, data]) => ({
        name,
        avgDaysToSell: Math.round(data.totalDays / data.count),
        itemsSold: data.count
      }))
      .filter(v => v.itemsSold >= 3) // Only include vendors with enough data
      .sort((a, b) => a.avgDaysToSell - b.avgDaysToSell);
    
    // ========== STEP 6: Calculate dead stock ==========
    const deadStockItems = [];
    let total60Days = 0, total90Days = 0, total120Days = 0;
    let value60Days = 0, value90Days = 0, value120Days = 0;
    
    for (const inv of inventoryData.results || []) {
      if (!inv.item_id) continue;
      const qty = inv.qty_on_hand || 0;
      if (qty <= 0) continue;
      
      const details = itemDetails[inv.item_id];
      const receiveInfo = itemReceiveInfo[inv.item_id];
      const sales = itemSales[inv.item_id];
      
      if (!details) continue;
      
      // Calculate days stagnant
      let daysStagnant = 0;
      let lastSaleDate = null;
      
      if (sales && sales.length > 0) {
        const lastSale = sales.sort((a, b) => new Date(b.date) - new Date(a.date))[0];
        lastSaleDate = lastSale.date;
        daysStagnant = Math.floor((now - new Date(lastSaleDate)) / (1000 * 60 * 60 * 24));
      } else if (receiveInfo) {
        daysStagnant = Math.floor((now - new Date(receiveInfo.receiveDate)) / (1000 * 60 * 60 * 24));
      } else {
        continue;
      }
      
      if (daysStagnant < 60) continue;
      
      const cost = details.cost || inv.unit_cost || 0;
      const value = qty * cost;
      
      let suggestedMarkdown = '20% off';
      if (daysStagnant >= 120) {
        suggestedMarkdown = '40% off';
        total120Days++;
        value120Days += value;
      } else if (daysStagnant >= 90) {
        suggestedMarkdown = '30% off';
        total90Days++;
        value90Days += value;
      } else {
        total60Days++;
        value60Days += value;
      }
      
      deadStockItems.push({
        id: inv.item_id,
        name: details.name,
        category: details.category,
        vendor: details.vendor,
        qty: qty,
        price: details.price,
        value: Math.round(value * 100) / 100,
        daysStagnant: daysStagnant,
        lastSaleDate: lastSaleDate ? lastSaleDate.split('T')[0] : null,
        suggestedMarkdown: suggestedMarkdown
      });
    }
    
    deadStockItems.sort((a, b) => b.daysStagnant - a.daysStagnant);
    
    // ========== STEP 7: Save to cache ==========
    const analysisData = {
      deadStock: {
        summary: {
          items60Days: total60Days,
          items90Days: total90Days,
          items120Days: total120Days,
          value60Days: Math.round(value60Days * 100) / 100,
          value90Days: Math.round(value90Days * 100) / 100,
          value120Days: Math.round(value120Days * 100) / 100,
          totalItems: total60Days + total90Days + total120Days,
          totalValue: Math.round((value60Days + value90Days + value120Days) * 100) / 100,
        },
        items: deadStockItems.slice(0, 100) // Limit to top 100
      },
      velocity: {
        byCategory: categoryVelocity,
        byVendor: vendorVelocity
      },
      stats: {
        totalItemsAnalyzed: itemCount,
        totalReceipts: receiptsData.results?.length || 0,
        totalSalesRecords: salesData.results?.length || 0
      }
    };
    
    await pool.query(
      `INSERT INTO inventory_cache (cache_key, data, synced_at)
       VALUES ('inventory_analysis', $1, CURRENT_TIMESTAMP)
       ON CONFLICT (cache_key)
       DO UPDATE SET data = $1, synced_at = CURRENT_TIMESTAMP`,
      [JSON.stringify(analysisData)]
    );
    
    const duration = Math.round((Date.now() - startTime) / 1000);
    console.log(`Inventory sync complete in ${duration}s`);
    
    res.json({
      success: true,
      duration: `${duration}s`,
      stats: analysisData.stats
    });
  } catch (error) {
    console.error('Error syncing inventory:', error);
    res.status(500).json({ error: error.message });
  }
});

// Health check endpoint
app.get('/api/health', async (req, res) => {
  try {
    // Test Heartland connection
    await heartlandRequest('/system/whoami');
    
    // Test database connection
    await pool.query('SELECT 1');
    
    res.json({ 
      status: 'ok',
      heartland: 'connected',
      database: 'connected',
      anthropic: process.env.ANTHROPIC_API_KEY ? 'configured' : 'missing',
    });
  } catch (error) {
    res.json({ 
      status: 'degraded',
      heartland: 'error: ' + error.message,
      database: 'unknown',
      anthropic: process.env.ANTHROPIC_API_KEY ? 'configured' : 'missing',
    });
  }
});

// Debug endpoint to see processed items in database
app.get('/api/debug/processed-items', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM processed_items ORDER BY updated_at DESC LIMIT 50');
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Debug endpoint to see raw item data from Heartland
app.get('/api/debug/item/:id', async (req, res) => {
  try {
    const item = await heartlandRequest(`/items/${req.params.id}`);
    res.json(item);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Debug endpoint to see raw receipt line data from Heartland
app.get('/api/debug/receipt/:id', async (req, res) => {
  try {
    const receipt = await heartlandRequest(`/purchasing/receipts/${req.params.id}`);
    const lines = await heartlandRequest(`/purchasing/receipts/${req.params.id}/lines?per_page=10`);
    res.json({ receipt, lines });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Debug endpoint to check sales/reporting API
app.get('/api/debug/sales', async (req, res) => {
  try {
    // Try multiple endpoints to see what works
    const results = {};
    
    // Try 1: /sales/tickets
    try {
      const tickets = await heartlandRequest(`/sales/tickets?per_page=3`);
      results.tickets = tickets;
    } catch (e) {
      results.tickets_error = e.message;
    }
    
    // Try 2: /sales/transactions  
    try {
      const transactions = await heartlandRequest(`/sales/transactions?per_page=3`);
      results.transactions = transactions;
    } catch (e) {
      results.transactions_error = e.message;
    }
    
    // Try 3: /reporting/sales
    try {
      const reportingSales = await heartlandRequest(`/reporting/sales?per_page=3`);
      results.reporting_sales = reportingSales;
    } catch (e) {
      results.reporting_sales_error = e.message;
    }
    
    res.json(results);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Debug endpoint to check sales for a specific item
app.get('/api/debug/item-sales/:itemId', async (req, res) => {
  try {
    // Get sales for specific item
    const sales = await heartlandRequest(`/reporting/analyzer?metrics[]=item_qty_sold&metrics[]=item_total&group[]=item_id&filter[item_id]=${req.params.itemId}&per_page=10`);
    res.json({ sales });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`BoutiqueFlow running at http://localhost:${PORT}`);
  console.log(`Heartland API: ${HEARTLAND_BASE_URL}`);
});
